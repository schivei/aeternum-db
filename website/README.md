# AeternumDB Documentation Website

This directory contains the [Docusaurus](https://docusaurus.io/) documentation site for AeternumDB.

## Local Development

### Prerequisites

- Node.js ≥ 18
- npm ≥ 9

### Install Dependencies

```bash
npm install
```

### Start Dev Server

```bash
npm start
```

Opens `http://localhost:3000/aeternum-db/` in your browser with hot-reload.

### Build

```bash
npm run build
```

Generates a static build in `build/`. Serve it locally with:

```bash
npm run serve
```

## Structure

```
website/
├── docs/                     # Documentation pages (Markdown/MDX)
│   ├── intro.md              # Introduction page
│   ├── getting-started/      # Installation, quick start, configuration
│   ├── guides/               # SQL reference, query optimization, EXPLAIN, BTree
│   ├── architecture/         # Storage engine, BTree design, query optimizer
│   ├── api/                  # Core types, StorageEngine, BTree, QueryPlanner
│   └── community/            # Contributing, code of conduct, security, roadmap
├── src/
│   ├── css/custom.css        # Global CSS overrides
│   └── pages/index.js        # Homepage
├── static/
│   └── img/                  # Static assets (logo, favicon)
├── docusaurus.config.js      # Site configuration
└── sidebars.js               # Sidebar navigation
```

## Deployment

The site is deployed to GitHub Pages via the CI/CD pipeline.

Manual deploy:

```bash
GIT_USER=<your-github-username> npm run deploy
```
