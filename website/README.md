# GrainFS website

Static landing page for `https://grainfs.gritive.com`.

## Local preview

```bash
python3 -m http.server 4173 --directory website
```

Then open <http://localhost:4173>.

## Cloudflare Pages

Project name: `grainfs`
Production branch: `master`
Static output directory: `website`
Custom domain: `grainfs.gritive.com`

Deploy manually from the repository root:

```bash
wrangler pages deploy website --project-name grainfs --branch master
```

GitHub Actions deployment is configured in `.github/workflows/deploy-website.yml`.
The workflow needs these repository secrets:

- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_API_TOKEN` with Cloudflare Pages edit/deploy permissions
