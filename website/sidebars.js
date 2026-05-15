// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  mainSidebar: [
    {
      type: 'doc',
      id: 'intro',
      label: 'Introduction',
    },
    ...([
      ['Getting Started', [
        'getting-started/installation',
        'getting-started/quick-start',
        'getting-started/configuration',
      ]],
      ['SQL Guides', [
        'guides/sql-reference',
        'guides/query-optimization',
        'guides/explain',
        'guides/btree-usage',
      ]],
      ['Architecture', [
        'architecture/storage',
        'architecture/btree-design',
        'architecture/query-optimizer',
      ]],
      ['API Reference', [
        'api/core-types',
        'api/storage-engine',
        'api/btree',
        'api/query-planner',
      ]],
      ['Community', [
        'community/contributing',
        'community/code-of-conduct',
        'community/security',
        'community/roadmap',
      ]],
    ].map(([label, items]) => ({ type: 'category', label, items }))),
  ],
};

export default sidebars;
