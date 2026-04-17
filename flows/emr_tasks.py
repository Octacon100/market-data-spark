"""
EMR Cluster Management Tasks for Market Data Pipeline
Handles complete EMR lifecycle: create, submit jobs, terminate
"""

from prefect import task, flow
from prefect.artifacts import create_markdown_artifact, create_link_artifact
from prefect.events import emit_event
import boto3
import time
import os
from typing import Dict, List, Optional
from datetime import datetime
from config_utils import resolve, make_boto3_client

# Configuration
S3_BUCKET = resolve('s3-bucket', 'S3_BUCKET')
AWS_REGION = resolve('aws-region', 'AWS_DEFAULT_REGION') or os.getenv('AWS_REGION', 'us-east-1')
EMR_KEY_PAIR = resolve('emr-key-pair', 'EMR_KEY_PAIR') or 'your-key-pair'
USE_SPOT_INSTANCES = (resolve('use-spot-instances', 'USE_SPOT_INSTANCES') or 'True').lower() == 'true'
SPOT_BID_PRICE = resolve('spot-bid-price', 'SPOT_BID_PRICE') or '0.15'
EMR_RELEASE_LABEL = resolve('emr-release-label', 'EMR_RELEASE_LABEL') or 'emr-7.3.0'

# Initialize AWS clients
emr = make_boto3_client('emr', region=AWS_REGION)
s3 = make_boto3_client('s3', region=AWS_REGION)
ec2 = make_boto3_client('ec2', region=AWS_REGION)


def get_default_subnet() -> Optional[str]:
    """
    Get subnet from default VPC, or fall back to any available VPC
    Required for m5.xlarge instances
    
    Returns:
        str: Subnet ID or None
    """
    try:
        # Try 1: Get default VPC
        vpcs = ec2.describe_vpcs(Filters=[{'Name': 'isDefault', 'Values': ['true']}])
        
        if vpcs['Vpcs']:
            vpc_id = vpcs['Vpcs'][0]['VpcId']
            print(f"   Found default VPC: {vpc_id}")
        else:
            # Try 2: Fall back to ANY VPC
            print("   No default VPC found, looking for any available VPC...")
            all_vpcs = ec2.describe_vpcs()
            
            if not all_vpcs['Vpcs']:
                print("[WARNING]️  No VPCs found at all!")
                return None
            
            vpc_id = all_vpcs['Vpcs'][0]['VpcId']
            print(f"   Using VPC: {vpc_id} (not default, but available)")
        
        # Get subnets in the VPC
        subnets = ec2.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}])
        
        if not subnets['Subnets']:
            print(f"[WARNING]️  No subnets found in VPC {vpc_id}")
            return None
        
        # Return first available subnet
        subnet_id = subnets['Subnets'][0]['SubnetId']
        az = subnets['Subnets'][0]['AvailabilityZone']
        print(f"   Using subnet: {subnet_id} (AZ: {az})")
        
        return subnet_id
        
    except Exception as e:
        print(f"[WARNING]️  Error getting subnet: {e}")
        return None


@task(
    retries=2,
    retry_delay_seconds=30,
    log_prints=True,
    tags=["emr", "infrastructure", "create"]
)
def create_emr_cluster_with_auto_terminate() -> str:
    """
    Create EMR cluster that auto-terminates after steps complete
    This is the MOST cost-efficient approach
    
    Returns:
        str: Cluster ID
    """
    
    print("[START] Creating transient EMR cluster...")
    print(f"   Release: {EMR_RELEASE_LABEL}")
    print(f"   Spot instances: {USE_SPOT_INSTANCES}")
    print(f"   S3 bucket: {S3_BUCKET}")
    
    # Get default subnet (required for m5.xlarge)
    subnet_id = get_default_subnet()
    if not subnet_id:
        raise ValueError(
            "No default VPC/subnet found. "
            "m5.xlarge instances require a VPC. "
            "Please create a default VPC or specify a subnet in .env"
        )
    
    # Instance configuration
    instance_groups = [
        {
            'Name': 'Master',
            'Market': 'ON_DEMAND',  # Master always on-demand for stability
            'InstanceRole': 'MASTER',
            'InstanceType': 'm5.xlarge',
            'InstanceCount': 1,
        }
    ]
    
    # Core nodes - use Spot if enabled
    if USE_SPOT_INSTANCES:
        instance_groups.append({
            'Name': 'Core',
            'Market': 'SPOT',
            'InstanceRole': 'CORE',
            'InstanceType': 'm5.xlarge',
            'InstanceCount': 2,
            'BidPrice': SPOT_BID_PRICE,
        })
    else:
        instance_groups.append({
            'Name': 'Core',
            'Market': 'ON_DEMAND',
            'InstanceRole': 'CORE',
            'InstanceType': 'm5.xlarge',
            'InstanceCount': 2,
        })
    
    try:
        response = emr.run_job_flow(
            Name=f'MarketData-Transient-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
            ReleaseLabel=EMR_RELEASE_LABEL,
            Applications=[
                {'Name': 'Spark'},
                {'Name': 'Hadoop'}
            ],
            Instances={
                'InstanceGroups': instance_groups,
                'Ec2SubnetId': subnet_id,  # Required for VPC instances
                'KeepJobFlowAliveWhenNoSteps': False,  # Auto-terminate after steps!
                'TerminationProtected': False,
                'Ec2KeyName': EMR_KEY_PAIR,
            },
            Steps=[
                # Submit all Spark jobs at cluster creation
                {
                    'Name': 'DailyAnalytics',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            '--master', 'yarn',
                            '--conf', 'spark.yarn.submit.waitAppCompletion=true',
                            f's3://{S3_BUCKET}/scripts/spark/daily_analytics.py',
                            '--bucket', S3_BUCKET
                        ]
                    }
                },
                {
                    'Name': 'MLFeatures',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            '--master', 'yarn',
                            '--conf', 'spark.yarn.submit.waitAppCompletion=true',
                            f's3://{S3_BUCKET}/scripts/spark/ml_features.py',
                            '--bucket', S3_BUCKET
                        ]
                    }
                },
                {
                    'Name': 'VolatilityMetrics',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            '--master', 'yarn',
                            '--conf', 'spark.yarn.submit.waitAppCompletion=true',
                            f's3://{S3_BUCKET}/scripts/spark/volatility_metrics.py',
                            '--bucket', S3_BUCKET
                        ]
                    }
                }
            ],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            LogUri=f's3://{S3_BUCKET}/emr-logs/',
            VisibleToAllUsers=True,
            Tags=[
                {'Key': 'Project', 'Value': 'MarketDataPipeline'},
                {'Key': 'ManagedBy', 'Value': 'Prefect'},
                {'Key': 'Environment', 'Value': 'Production'}
            ]
        )
        
        cluster_id = response['JobFlowId']
        
        print(f"  ✅ Cluster created: {cluster_id}")
        print(f"  📋 Steps: DailyAnalytics, MLFeatures, VolatilityMetrics")
        print(f"  🔄 Cluster will auto-terminate when all steps complete")
        print(f"  [DATA] Monitor at: https://console.aws.amazon.com/emr/home?region={AWS_REGION}#/clusterDetails/{cluster_id}")
        
        # Create clickable artifact
        create_link_artifact(
            key=f"emr-cluster-{str(cluster_id).lower()}",
            link=f"https://console.aws.amazon.com/emr/home?region={AWS_REGION}#/clusterDetails/{cluster_id}",
            description=f"EMR Cluster {cluster_id} - Monitor progress"
        )
        
        # Emit event
        emit_event(
            event="emr.cluster.created",
            resource={
                "prefect.resource.id": f"emr-cluster-{cluster_id}",
                "prefect.resource.name": f"EMR Cluster {cluster_id}"
            },
            payload={
                "cluster_id": cluster_id,
                "release_label": EMR_RELEASE_LABEL,
                "use_spot": USE_SPOT_INSTANCES,
                "steps_count": 3
            }
        )
        
        return cluster_id
        
    except Exception as e:
        print(f"  ❌ Failed to create EMR cluster: {e}")
        raise


@task(
    retries=1,
    retry_delay_seconds=60,
    log_prints=True,
    tags=["emr", "monitoring"]
)
def wait_for_cluster_ready(cluster_id: str, timeout_minutes: int = 30):
    """
    Wait for EMR cluster to be in RUNNING state
    
    Args:
        cluster_id: EMR cluster ID
        timeout_minutes: Maximum time to wait
    """
    
    print(f"[WAIT] Waiting for cluster {cluster_id} to be ready...")
    
    try:
        waiter = emr.get_waiter('cluster_running')
        waiter.wait(
            ClusterId=cluster_id,
            WaiterConfig={
                'Delay': 30,  # Check every 30 seconds
                'MaxAttempts': timeout_minutes * 2  # 30 sec * 2 = 1 min
            }
        )
        
        print(f"  ✅ Cluster {cluster_id} is ready!")
        
        # Emit event
        emit_event(
            event="emr.cluster.ready",
            resource={"prefect.resource.id": f"emr-cluster-{cluster_id}"},
            payload={"cluster_id": cluster_id}
        )
        
    except Exception as e:
        print(f"  [WARNING]️ Timeout waiting for cluster: {e}")
        raise


@task(
    log_prints=True,
    tags=["emr", "monitoring"]
)
def monitor_emr_steps(cluster_id: str) -> Dict[str, str]:
    """
    Monitor EMR step execution (non-blocking)
    Returns step statuses
    
    Args:
        cluster_id: EMR cluster ID
        
    Returns:
        Dict mapping step names to statuses
    """
    
    print(f"[DATA] Checking step statuses for cluster {cluster_id}...")
    
    try:
        # List all steps
        response = emr.list_steps(ClusterId=cluster_id)
        
        step_statuses = {}
        
        for step in response.get('Steps', []):
            step_name = step['Name']
            step_state = step['Status']['State']
            
            step_statuses[step_name] = step_state
            
            # Print status with emoji
            if step_state == 'COMPLETED':
                print(f"  ✅ {step_name}: {step_state}")
            elif step_state == 'RUNNING':
                print(f"  🔄 {step_name}: {step_state}")
            elif step_state in ['PENDING', 'CANCEL_PENDING']:
                print(f"  [WAIT] {step_name}: {step_state}")
            elif step_state == 'FAILED':
                print(f"  ❌ {step_name}: {step_state}")
            else:
                print(f"  [WARNING]️  {step_name}: {step_state}")
        
        return step_statuses
        
    except Exception as e:
        print(f"  [WARNING]️ Error checking steps: {e}")
        return {}


@task(
    log_prints=True,
    tags=["emr", "cleanup"]
)
def check_cluster_status(cluster_id: str) -> str:
    """
    Check if cluster has terminated
    
    Args:
        cluster_id: EMR cluster ID
        
    Returns:
        Cluster state
    """
    
    try:
        response = emr.describe_cluster(ClusterId=cluster_id)
        state = response['Cluster']['Status']['State']
        
        print(f"[DATA] Cluster {cluster_id} state: {state}")
        
        return state
        
    except Exception as e:
        print(f"  [WARNING]️ Error checking cluster: {e}")
        return "UNKNOWN"


@task(
    log_prints=True,
    tags=["emr", "cleanup"]
)
def terminate_emr_cluster(cluster_id: str):
    """
    Manually terminate EMR cluster
    Only needed if cluster didn't auto-terminate
    
    Args:
        cluster_id: EMR cluster ID
    """
    
    print(f"🛑 Manually terminating cluster {cluster_id}...")
    
    try:
        emr.terminate_job_flows(JobFlowIds=[cluster_id])
        
        print(f"  ✅ Termination initiated for {cluster_id}")
        
        # Emit event
        emit_event(
            event="emr.cluster.terminated",
            resource={"prefect.resource.id": f"emr-cluster-{cluster_id}"},
            payload={
                "cluster_id": cluster_id,
                "termination_type": "manual"
            }
        )
        
    except Exception as e:
        print(f"  [WARNING]️ Error terminating cluster: {e}")
        # Don't raise - termination errors shouldn't fail the flow


@flow(
    name="spark-analytics-on-emr",
    description="Run Spark analytics on auto-terminating EMR cluster",
    log_prints=True
)
def spark_analytics_flow():
    """
    Complete Spark analytics workflow:
    1. Create EMR cluster with all steps
    2. Wait for cluster to be ready
    3. Monitor step execution (async)
    4. Cluster auto-terminates when done
    """
    
    print("\n" + "="*60)
    print("⚡ Starting Spark Analytics on EMR")
    print("="*60 + "\n")
    
    start_time = datetime.now()
    
    try:
        # Step 1: Create cluster with all Spark jobs
        cluster_id = create_emr_cluster_with_auto_terminate()
        
        # Step 2: Wait for cluster to be ready
        wait_for_cluster_ready(cluster_id)
        
        # Step 3: Monitor steps (non-blocking check)
        step_statuses = monitor_emr_steps(cluster_id)
        
        # Step 4: Create summary artifact
        duration = (datetime.now() - start_time).total_seconds() / 60
        
        create_markdown_artifact(
            key=f"emr-spark-summary-{cluster_id}",
            markdown=f"""
# Spark Analytics on EMR - Started

## Cluster Details
- **Cluster ID**: `{cluster_id}`
- **Release**: {EMR_RELEASE_LABEL}
- **Spot Instances**: {USE_SPOT_INSTANCES}
- **Start Time**: {start_time.strftime('%Y-%m-%d %H:%M:%S')}

## Steps Submitted
1. **DailyAnalytics** - Daily statistics computation
2. **MLFeatures** - ML feature engineering  
3. **VolatilityMetrics** - Volatility analysis

## Current Status
{chr(10).join([f"- {name}: {status}" for name, status in step_statuses.items()])}

## Monitoring
- **EMR Console**: [View Cluster](https://console.aws.amazon.com/emr/home?region={AWS_REGION}#/clusterDetails/{cluster_id})
- **S3 Logs**: `s3://{S3_BUCKET}/emr-logs/{cluster_id}/`
- **Results**: `s3://{S3_BUCKET}/analytics/`

## Auto-Termination
Cluster will automatically terminate when all steps complete (typically 10-15 minutes).

## Estimated Costs
- **Cluster Runtime**: ~15 minutes
- **Estimated Cost**: $0.15 - $0.25
            """,
            description="EMR Spark analytics job summary"
        )
        
        print(f"\n✅ EMR cluster created and running!")
        print(f"   Cluster ID: {cluster_id}")
        print(f"   Steps: 3 Spark jobs submitted")
        print(f"   Auto-terminate: Enabled")
        print(f"   Duration so far: {duration:.1f} minutes")
        print(f"\n[DATA] Monitor progress at:")
        print(f"   https://console.aws.amazon.com/emr/home?region={AWS_REGION}#/clusterDetails/{cluster_id}")
        print(f"\n💡 Cluster will auto-terminate when jobs complete")
        print("="*60 + "\n")
        
        return {
            'cluster_id': cluster_id,
            'start_time': start_time.isoformat(),
            'step_statuses': step_statuses
        }
        
    except Exception as e:
        print(f"\n❌ Error in Spark analytics: {e}")
        raise


if __name__ == "__main__":
    # For testing
    spark_analytics_flow()
