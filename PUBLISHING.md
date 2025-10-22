# Publishing to Grafana Plugin Marketplace

This guide explains how to publish the Arc datasource to the Grafana plugin marketplace.

## Overview

There are **two distribution methods**:

1. **Unsigned Plugin** - For internal/private use
2. **Signed Plugin** - For public Grafana marketplace

## Option 1: Unsigned Plugin (Internal Use)

### Advantages
- ✅ No approval process required
- ✅ Can be installed immediately
- ✅ Full control over updates
- ✅ Good for testing and internal deployments

### Disadvantages
- ❌ Not in official marketplace
- ❌ Requires Grafana config change to allow unsigned plugins
- ❌ Manual installation needed
- ❌ No automatic updates

### Installation (Unsigned)

Users need to configure Grafana to allow unsigned plugins:

**grafana.ini:**
```ini
[plugins]
allow_loading_unsigned_plugins = basekick-arc-datasource
```

**Docker:**
```bash
docker run -d \
  -e "GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=basekick-arc-datasource" \
  grafana/grafana:latest
```

**Installation:**
```bash
# Download release
wget https://github.com/basekick-labs/grafana-arc-datasource/releases/download/v1.0.0/basekick-arc-datasource-1.0.0.zip

# Extract to plugins directory
unzip basekick-arc-datasource-1.0.0.zip -d /var/lib/grafana/plugins/

# Restart Grafana
systemctl restart grafana-server
```

---

## Option 2: Signed Plugin (Public Marketplace)

### Advantages
- ✅ Listed in official Grafana plugin catalog
- ✅ Trusted by all Grafana instances
- ✅ Users can install via UI
- ✅ Automatic update notifications
- ✅ Professional credibility

### Disadvantages
- ❌ Requires approval from Grafana Labs
- ❌ Review process can take time
- ❌ Must meet quality standards
- ❌ Requires maintaining plugin signature

### Prerequisites

1. **Grafana.com Account**
   - Create account at https://grafana.com
   - Join as organization member

2. **Plugin Signature**
   - Required for all public plugins
   - Obtained from Grafana Labs

3. **Quality Requirements**
   - Working plugin with no critical bugs
   - Comprehensive documentation
   - Screenshots/demo
   - Semantic versioning
   - Changelog

### Step 1: Prepare Plugin for Submission

#### 1.1 Update plugin.json

Ensure all metadata is complete:

```json
{
  "type": "datasource",
  "name": "Arc",
  "id": "basekick-arc-datasource",

  "info": {
    "description": "High-performance datasource for Arc time-series database using Apache Arrow",
    "author": {
      "name": "Basekick Labs",
      "url": "https://github.com/basekick-labs"
    },
    "keywords": [
      "arc",
      "timeseries",
      "arrow",
      "sql",
      "columnar",
      "analytics"
    ],
    "logos": {
      "small": "img/logo.svg",
      "large": "img/logo.svg"
    },
    "links": [
      {
        "name": "GitHub",
        "url": "https://github.com/basekick-labs/grafana-arc-datasource"
      },
      {
        "name": "Arc Documentation",
        "url": "https://github.com/basekick-labs/arc"
      }
    ],
    "screenshots": [
      {
        "name": "Query Editor",
        "path": "img/screenshot-query-editor.png"
      },
      {
        "name": "Configuration",
        "path": "img/screenshot-config.png"
      },
      {
        "name": "Dashboard Example",
        "path": "img/screenshot-dashboard.png"
      }
    ],
    "version": "1.0.0",
    "updated": "2025-10-22"
  },

  "dependencies": {
    "grafanaDependency": ">=10.0.0",
    "plugins": []
  }
}
```

#### 1.2 Create Screenshots

Take screenshots of:
1. **Query Editor** - Show SQL editor with example query
2. **Configuration** - Datasource settings page
3. **Dashboard** - Working dashboard with Arc data
4. **Variable Editor** - Template variable configuration (optional)

Save to `src/img/` directory at **1920x1080** resolution.

#### 1.3 Create Logo

- **Format:** SVG (vector)
- **Size:** Square (1:1 ratio)
- **Colors:** Match Arc branding
- **Files needed:**
  - `src/img/logo.svg` - Main logo
  - `src/img/logo_light.svg` - Light theme variant (optional)
  - `src/img/logo_dark.svg` - Dark theme variant (optional)

#### 1.4 Update Documentation

Ensure these files are complete:
- ✅ `README.md` - Installation, usage, examples
- ✅ `CHANGELOG.md` - Version history
- ✅ `LICENSE` - Apache 2.0 license
- ✅ `CONTRIBUTING.md` - Contribution guidelines

### Step 2: Get Access Token from Grafana

1. **Go to Grafana Cloud Portal**
   - Visit: https://grafana.com/
   - Sign in to your account

2. **Create Organization** (if needed)
   - Organization Settings → Create Organization
   - Name: "Basekick Labs" (or your org name)

3. **Generate Access Token**
   - Go to: https://grafana.com/orgs/<your-org>/access-policies
   - Click "Create access policy"
   - Name: "Arc Datasource Plugin Signing"
   - Scopes: Select `plugins:write`
   - Click "Create"
   - **Save the token** - you won't see it again!

4. **Add Token to GitHub Secrets**
   ```bash
   # In GitHub repository:
   # Settings → Secrets and variables → Actions → New repository secret

   Name: GRAFANA_ACCESS_POLICY_TOKEN
   Value: <paste your token>
   ```

### Step 3: Sign the Plugin

Signing happens automatically via GitHub Actions when you create a release tag.

**Manual Signing (for testing):**

```bash
# Install signing tool
npm install -g @grafana/sign-plugin

# Set environment variable
export GRAFANA_ACCESS_POLICY_TOKEN=<your-token>

# Sign plugin
npx @grafana/sign-plugin
```

This creates `MANIFEST.txt` in the `dist/` directory with signature.

### Step 4: Build Release

1. **Update Version**

   Update version in both files:
   - `package.json`: `"version": "1.0.0"`
   - `src/plugin.json`: `"info.version": "1.0.0"`

2. **Build Plugin**
   ```bash
   # Build frontend
   npm install
   npm run build

   # Build backend for all platforms
   mage buildAll

   # Sign plugin (if you have token)
   export GRAFANA_ACCESS_POLICY_TOKEN=<your-token>
   npm run sign
   ```

3. **Validate Plugin**
   ```bash
   # Install validator
   git clone https://github.com/grafana/plugin-validator
   cd plugin-validator/pkg/cmd/plugincheck2
   go install

   # Run validation
   plugincheck2 -config ../../../config/default.yaml /path/to/your/plugin
   ```

4. **Create Release Archive**
   ```bash
   # Package plugin
   cd /path/to/grafana-arc-datasource
   mv dist basekick-arc-datasource
   zip -r basekick-arc-datasource-1.0.0.zip basekick-arc-datasource

   # Generate checksum
   md5sum basekick-arc-datasource-1.0.0.zip > basekick-arc-datasource-1.0.0.zip.md5
   ```

### Step 5: Create GitHub Release

1. **Create Git Tag**
   ```bash
   git tag -a v1.0.0 -m "Release version 1.0.0"
   git push origin v1.0.0
   ```

2. **GitHub Actions** will automatically:
   - Build frontend and backend
   - Sign plugin (if token is set)
   - Run tests
   - Create draft release
   - Upload artifacts

3. **Edit Release**
   - Go to: https://github.com/basekick-labs/grafana-arc-datasource/releases
   - Edit the draft release
   - Add release notes from CHANGELOG.md
   - Publish release

### Step 6: Submit to Grafana Marketplace

1. **Go to Plugin Submission Page**
   - Visit: https://grafana.com/auth/sign-in
   - Navigate to: My Account → My Plugins → Submit Plugin

2. **Fill Out Submission Form**

   **Plugin Information:**
   - Plugin ID: `basekick-arc-datasource`
   - Plugin Type: `Datasource`
   - Version: `1.0.0`

   **Repository:**
   - GitHub URL: `https://github.com/basekick-labs/grafana-arc-datasource`
   - Release URL: `https://github.com/basekick-labs/grafana-arc-datasource/releases/tag/v1.0.0`

   **Archive:**
   - Upload: `basekick-arc-datasource-1.0.0.zip`
   - MD5 Checksum: (from .md5 file)

3. **Provide Additional Information**

   - **Category:** Time Series Databases
   - **Description:** (from README.md)
   - **Screenshots:** Upload from `src/img/`
   - **Demo/Video:** (optional, but recommended)
   - **Support Contact:** support@basekick.com (or your email)

4. **Submit for Review**
   - Review all information
   - Accept terms and conditions
   - Click "Submit"

### Step 7: Wait for Review

**Timeline:**
- Initial review: 1-3 business days
- Feedback/changes: Varies
- Final approval: 1-2 business days

**Review Criteria:**
- ✅ Plugin builds and runs without errors
- ✅ No security vulnerabilities
- ✅ Documentation is complete
- ✅ Screenshots are clear
- ✅ Follows Grafana plugin guidelines
- ✅ Semantic versioning
- ✅ Proper licensing

**Possible Outcomes:**
- **Approved** - Plugin published to marketplace 🎉
- **Changes Requested** - Fix issues and resubmit
- **Rejected** - Review feedback and address concerns

### Step 8: After Approval

Once approved:

1. **Plugin Goes Live**
   - Listed at: https://grafana.com/grafana/plugins/basekick-arc-datasource
   - Users can install via Grafana UI

2. **Monitor Usage**
   - Check download stats
   - Monitor GitHub issues
   - Respond to community feedback

3. **Plan Updates**
   - Bug fixes → Patch versions (1.0.1)
   - New features → Minor versions (1.1.0)
   - Breaking changes → Major versions (2.0.0)

---

## Publishing Updates

### Patch Release (Bug Fixes)

```bash
# Update version
npm version patch  # 1.0.0 → 1.0.1

# Build and test
npm run build
mage buildAll
npm run test:ci

# Create release
git tag -a v1.0.1 -m "Bug fixes"
git push origin v1.0.1

# Submit update to Grafana (automated after first approval)
```

### Minor Release (New Features)

```bash
# Update version
npm version minor  # 1.0.1 → 1.1.0

# Update CHANGELOG.md with new features

# Build and test
npm run build
mage buildAll

# Create release
git tag -a v1.1.0 -m "New features"
git push origin v1.1.0
```

### Major Release (Breaking Changes)

```bash
# Update version
npm version major  # 1.1.0 → 2.0.0

# Update CHANGELOG.md with breaking changes
# Update migration guide

# Build and test thoroughly
npm run build
mage buildAll
npm run test:ci

# Create release
git tag -a v2.0.0 -m "Major release with breaking changes"
git push origin v2.0.0

# May require resubmission to Grafana for review
```

---

## Troubleshooting

### Signing Fails

**Error:** `Failed to sign plugin`

**Solution:**
1. Verify token is valid: https://grafana.com/orgs/<your-org>/access-policies
2. Check token has `plugins:write` scope
3. Ensure token is in GitHub secrets or environment variable

### Validation Fails

**Error:** `Plugin validation failed`

**Solution:**
1. Run validator locally: `plugincheck2 -config config/default.yaml dist/`
2. Fix reported issues
3. Common issues:
   - Missing MANIFEST.txt (need to sign)
   - Invalid plugin.json
   - Missing required files

### Marketplace Submission Rejected

**Common Reasons:**
- Incomplete documentation
- Missing or poor screenshots
- Security vulnerabilities
- Not following Grafana guidelines
- Broken functionality

**Solution:**
- Review feedback carefully
- Fix all issues
- Resubmit with explanation of changes

---

## Recommended Workflow

### For Internal Testing (Unsigned)

```bash
# Quick iteration
npm run dev
mage build

# Install locally
ln -s $(pwd)/dist /var/lib/grafana/plugins/grafana-arc-datasource

# Configure Grafana for unsigned
echo "allow_loading_unsigned_plugins = basekick-arc-datasource" >> /etc/grafana/grafana.ini
```

### For Production Release (Signed)

```bash
# 1. Complete development
git checkout main
git pull

# 2. Update version and changelog
npm version minor
# Edit CHANGELOG.md

# 3. Build and test
npm run build
mage buildAll
npm run test:ci

# 4. Sign (if you have token)
export GRAFANA_ACCESS_POLICY_TOKEN=<token>
npm run sign

# 5. Create release
git tag -a v1.1.0 -m "Release 1.1.0"
git push origin v1.1.0

# 6. GitHub Actions builds and creates draft release
# 7. Edit release notes and publish
# 8. Submit to Grafana marketplace (first time only)
```

---

## Resources

- **Grafana Plugin Development:** https://grafana.com/docs/grafana/latest/developers/plugins/
- **Plugin Signing:** https://grafana.com/docs/grafana/latest/developers/plugins/sign-a-plugin/
- **Marketplace Guidelines:** https://grafana.com/docs/grafana/latest/developers/plugins/publish-a-plugin/
- **Plugin Validator:** https://github.com/grafana/plugin-validator
- **Example Plugins:** https://github.com/grafana/grafana-starter-datasource-backend

## Questions?

- **Technical Issues:** GitHub Issues
- **Marketplace Questions:** plugins@grafana.com
- **Security Concerns:** security@grafana.com
