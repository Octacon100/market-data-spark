# Windows PowerShell Quick Reference

## 🪟 Running the Pipeline on Windows

### One-Time Setup (5 minutes)

```powershell
# 1. Extract the ZIP file
# Right-click market-data-spark-emr.zip → Extract All

# 2. Open PowerShell (as Administrator recommended)
# Press Win + X → Windows PowerShell (Admin)

# 3. Navigate to project
cd C:\path\to\market-data-spark-emr

# 4. Run automated setup
.\setup\powershell\quickstart.ps1
```

### Daily Usage

**Terminal 1 - Start Worker:**
```powershell
prefect worker start --pool local-laptop
```

**Terminal 2 - Run Pipeline:**
```powershell
python flows\market_data_with_spark.py
```

---

## 🔧 PowerShell Scripts Included

### `quickstart.ps1` - Complete Automated Setup
Does everything:
- ✓ Checks prerequisites (Python, AWS CLI, Git)
- ✓ Installs Python packages
- ✓ Creates/validates .env file
- ✓ Sets up AWS (S3 bucket, EMR roles, key pairs)
- ✓ Configures Prefect Cloud
- ✓ Uploads Spark scripts to S3

**Usage:**
```powershell
.\setup\powershell\quickstart.ps1

# Skip specific steps:
.\setup\powershell\quickstart.ps1 -SkipInstall   # Skip pip install
.\setup\powershell\quickstart.ps1 -SkipAWS       # Skip AWS setup
.\setup\powershell\quickstart.ps1 -SkipPrefect   # Skip Prefect setup
```

### `setup-emr.ps1` - AWS/EMR Only Setup
Just AWS and EMR configuration:
- ✓ Verifies AWS CLI and credentials
- ✓ Creates S3 bucket
- ✓ Creates EMR default roles
- ✓ Creates EC2 key pair
- ✓ Saves .pem file for SSH access

**Usage:**
```powershell
.\setup\powershell\setup-emr.ps1
```

---

## 🎨 PowerShell Tips

### Enable Script Execution (if needed)
```powershell
# Check current policy
Get-ExecutionPolicy

# If it's "Restricted", temporarily allow scripts:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Run script
.\setup\powershell\quickstart.ps1

# (Optional) Restore restriction after setup
Set-ExecutionPolicy -ExecutionPolicy Restricted -Scope CurrentUser
```

### Color-Coded Output
Scripts use colors for clarity:
- 🟢 **Green** = Success
- 🟡 **Yellow** = Warning/Info
- 🔴 **Red** = Error
- 🔵 **Cyan** = Headers
- ⚪ **Gray** = Details

### Environment Variables
PowerShell loads from `.env` automatically:
```powershell
# Scripts read .env and set for current session
# You can also manually set:
$env:S3_BUCKET = "my-bucket-name"
$env:ALPHA_VANTAGE_API_KEY = "ABC123"
```

### Common Commands Translation

| Linux/Bash | Windows PowerShell |
|------------|-------------------|
| `ls` | `Get-ChildItem` or `ls` |
| `cat file.txt` | `Get-Content file.txt` |
| `export VAR=value` | `$env:VAR = "value"` |
| `which python` | `Get-Command python` |
| `./script.sh` | `.\script.ps1` |
| `chmod +x` | _(not needed in Windows)_ |

---

## 🐛 Troubleshooting Windows Issues

### Issue: "Cannot run script - execution policy"
**Solution:**
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Issue: "Python command not found"
**Solution:**
1. Verify Python is installed: `python --version`
2. If not found, add to PATH:
   - Settings → System → About → Advanced system settings
   - Environment Variables → System Variables → Path → Edit
   - Add: `C:\Users\YourName\AppData\Local\Programs\Python\Python311`

### Issue: "AWS CLI not found"
**Solution:**
```powershell
# Install AWS CLI for Windows
# Download from: https://aws.amazon.com/cli/
# Or use MSI installer:
msiexec.exe /i https://awscli.amazonaws.com/AWSCLIV2.msi
```

### Issue: ".env file encoding issues"
**Solution:**
```powershell
# Save .env as UTF-8 without BOM in Notepad++/VS Code
# Or recreate from template:
Copy-Item .env.example .env
notepad .env
```

### Issue: "Path separator issues"
Python scripts handle both `/` and `\` automatically, but if you need:
```powershell
# Use Join-Path for cross-platform paths
$path = Join-Path "flows" "market_data_flow.py"

# Or normalize paths
$path = [System.IO.Path]::Combine("flows", "market_data_flow.py")
```

---

## 📂 File Structure (Windows Paths)

```
market-data-spark-emr\
├── setup\
│   ├── powershell\
│   │   ├── quickstart.ps1           # ⭐ Start here
│   │   └── setup-emr.ps1             # AWS/EMR only
│   └── upload_spark_scripts.py
├── flows\
│   ├── market_data_flow.py
│   ├── emr_tasks.py
│   ├── market_data_with_spark.py
│   └── deploy.py
├── spark_jobs\
│   ├── daily_analytics.py
│   ├── ml_features.py
│   └── volatility_metrics.py
├── .env                              # Your credentials
├── .env.example                      # Template
└── requirements.txt
```

---

## 🚀 Quick Start Checklist

- [ ] Extract ZIP file
- [ ] Open PowerShell (Admin)
- [ ] Run: `.\setup\powershell\quickstart.ps1`
- [ ] Edit `.env` with your credentials
- [ ] Run quickstart again to complete setup
- [ ] **Terminal 1:** Start worker
- [ ] **Terminal 2:** Run pipeline
- [ ] **Browser:** Monitor at https://app.prefect.cloud

---

## 💡 Pro Tips for Windows Users

1. **Use Windows Terminal** (modern, better than cmd.exe)
   - Download from Microsoft Store
   - Supports tabs, colors, Unicode

2. **VS Code Integrated Terminal**
   ```powershell
   # In VS Code: Ctrl + `
   # Already in project directory
   # Can switch between PowerShell, CMD, Git Bash
   ```

3. **Path Auto-Complete**
   ```powershell
   # Type .\set and press Tab
   # PowerShell auto-completes to .\setup\
   ```

4. **Command History**
   ```powershell
   # Up/Down arrows: Navigate history
   # Ctrl+R: Search history
   # history: Show all commands
   ```

5. **Aliases for Common Commands**
   ```powershell
   # Add to PowerShell profile for convenience
   Set-Alias -Name start-worker -Value "prefect worker start --pool local-laptop"
   Set-Alias -Name run-pipeline -Value "python flows\market_data_with_spark.py"
   ```

---

## 📞 Getting Help

**In PowerShell:**
```powershell
# Get help for any cmdlet
Get-Help Get-Content -Full

# List all commands
Get-Command

# Find commands
Get-Command *-Item
```

**For this project:**
- See `README.md` for complete documentation
- See `QUICKSTART.md` for step-by-step guide
- Check `troubleshooting` section in README

---

## 🎯 Remember

**Future conversations:** Just tell me:
> "I'm on Windows - use PowerShell for scripts"

And I'll default to PowerShell syntax! 

Or add to your PROJECT_BRIEF.md:
```markdown
## Development Environment
- OS: Windows 11
- Shell: PowerShell 7.x
- Python: 3.11+
```
