# Environment File Migration Notice

**Date**: 2025-12-26
**Status**: `.env` files have been moved to platform config structure

---

## What Changed?

The `.env` files that were previously in each scraper directory have been:
1. ✅ **Removed from git** (to protect secrets)
2. ✅ **Replaced with `.env.example` templates** (safe to commit)
3. ✅ **Migrated to platform config** (recommended location)

---

## Where Are My Secrets Now?

### Recommended Location (NEW)
Store your configuration in:
```
%USERPROFILE%\Documents\ScraperPlatform\config\{scraper_name}.env.json
```

Example:
- `Documents/ScraperPlatform/config/CanadaQuebec.env.json`
- `Documents/ScraperPlatform/config/Malaysia.env.json`
- `Documents/ScraperPlatform/config/Argentina.env.json`

**Benefits:**
- ✅ Secrets stored outside git repository
- ✅ Survives git operations (pull, reset, etc.)
- ✅ Works in packaged EXE mode
- ✅ Not CWD-dependent
- ✅ Centralized config management

### Legacy Location (STILL WORKS)
If you prefer, you can still use `.env` files in each scraper directory:
- `1. CanadaQuebec/.env`
- `2. Malaysia/.env`
- `3. Argentina/.env`

**Note:** These files are now in `.gitignore` and won't be committed.

---

## How to Migrate

### Option 1: Use Platform Config (Recommended)

1. Copy the `.env.example` template:
   ```bash
   # For CanadaQuebec
   copy "1. CanadaQuebec\.env.example" "%USERPROFILE%\Documents\ScraperPlatform\config\CanadaQuebec.env.json"
   ```

2. Edit the file and add your actual secrets:
   ```json
   {
     "scraper": {
       "id": "CanadaQuebec",
       "enabled": true
     },
     "config": {
       "OPENAI_MODEL": "gpt-4o-mini"
     },
     "secrets": {
       "OPENAI_API_KEY": "sk-proj-YOUR_ACTUAL_KEY_HERE"
     }
   }
   ```

3. Run the scraper - it will automatically find the config

### Option 2: Use Legacy .env Files

1. Copy the `.env.example` template:
   ```bash
   copy "1. CanadaQuebec\.env.example" "1. CanadaQuebec\.env"
   ```

2. Edit `.env` and add your actual secrets

3. **Important:** The `.env` file is now gitignored and won't be committed

---

## Configuration Precedence

The system now loads configuration in this order (highest to lowest priority):

1. **Runtime Overrides** (passed via command line or GUI)
2. **Environment Variables** (OS-level env vars)
3. **Scraper Config File** (`Documents/ScraperPlatform/config/{scraper}.env.json`)
4. **Legacy .env File** (`./{scraper}/.env`) - fallback only
5. **Platform Config** (`Documents/ScraperPlatform/config/platform.json`)
6. **Hardcoded Defaults** (in config_loader.py)

This means:
- You can override any setting via environment variables
- Platform config provides global defaults
- Scraper config overrides platform defaults
- Legacy .env files still work (backward compatible)

---

## What If I Had Secrets in the Old .env Files?

⚠️ **SECURITY NOTICE**

If you previously committed `.env` files with real secrets to git:

1. **Those secrets are compromised** - they're in git history
2. **You should rotate/regenerate them**:
   - OpenAI: Generate new API key at https://platform.openai.com/api-keys
   - AlfaBeta: Change your password
   - Proxies: Request new credentials from provider

3. **Optional:** Scrub git history (advanced):
   ```bash
   git filter-branch --force --index-filter \
     "git rm --cached --ignore-unmatch '*.env'" \
     --prune-empty --tag-name-filter cat -- --all
   ```
   **Warning:** This rewrites history. Coordinate with team.

---

## FAQ

**Q: Do I have to migrate to platform config?**
A: No, legacy `.env` files still work. But platform config is recommended for security and portability.

**Q: Can I use both?**
A: Yes! Platform config takes precedence, .env is fallback.

**Q: What if I don't have any secrets?**
A: You don't need any config files. The system will use defaults.

**Q: How do I verify my config is loaded correctly?**
A: Run the doctor command:
```bash
python platform_config.py doctor
```

**Q: My script can't find my config!**
A: Check these locations in order:
1. `%USERPROFILE%\Documents\ScraperPlatform\config\{scraper}.env.json`
2. `./{scraper}/.env`
3. Environment variables
4. If nothing found, defaults are used

---

## Need Help?

- See [MIGRATION.md](MIGRATION.md) for full migration guide
- See [INVENTORY.md](INVENTORY.md) for complete config inventory
- Run `python platform_config.py doctor` for diagnostics

---

**This change improves security and prepares the system for packaging as standalone EXE.**
