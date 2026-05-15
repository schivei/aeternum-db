// @ts-check
import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'AeternumDB',
  tagline: 'High-Performance, Extensible Database Management System built in C#',
  favicon: 'img/favicon.ico',

  url: 'https://schivei.github.io',
  baseUrl: '/aeternum-db/',

  organizationName: 'schivei',
  projectName: 'aeternum-db',

  onBrokenLinks: 'warn',

  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl:
            'https://github.com/schivei/aeternum-db/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'AeternumDB',
        logo: {
          alt: 'AeternumDB Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'mainSidebar',
            position: 'left',
            label: 'Documentation',
          },
          {
            href: 'https://github.com/schivei/aeternum-db',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Getting Started',
                to: '/docs/intro',
              },
              {
                label: 'Installation',
                to: '/docs/getting-started/installation',
              },
              {
                label: 'SQL Reference',
                to: '/docs/guides/sql-reference',
              },
            ],
          },
          {
            title: 'Architecture',
            items: [
              {
                label: 'Storage Engine',
                to: '/docs/architecture/storage',
              },
              {
                label: 'Query Optimizer',
                to: '/docs/architecture/query-optimizer',
              },
              {
                label: 'BTree Index',
                to: '/docs/architecture/btree-design',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/schivei/aeternum-db',
              },
              {
                label: 'Issues',
                href: 'https://github.com/schivei/aeternum-db/issues',
              },
              {
                label: 'Contributing',
                to: '/docs/community/contributing',
              },
              {
                label: 'License',
                href: 'https://github.com/schivei/aeternum-db/blob/main/LICENSE.md',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} AeternumDB Contributors. Built with Docusaurus.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['csharp', 'bash', 'powershell', 'json', 'sql'],
      },
      colorMode: {
        defaultMode: 'light',
        disableSwitch: false,
        respectPrefersColorScheme: true,
      },
      announcementBar: {
        id: 'announcement',
        content:
          '⭐️ AeternumDB is under active development — give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/schivei/aeternum-db">GitHub</a>! ⭐️',
        backgroundColor: '#20232a',
        textColor: '#fff',
        isCloseable: true,
      },
    }),
};

export default config;
