import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import PropTypes from 'prop-types';

import styles from './index.module.css';

function HomepageHeader() {
    const { siteConfig } = useDocusaurusContext();
    return (
        <header className={clsx('hero hero--primary', styles.heroBanner)}>
            <div className="container">
                <div className={styles.logoRow}>
                    <span className={styles.logoEmoji}>🌀</span>
                </div>
                <Heading as="h1" className="hero__title">
                    {siteConfig.title}
                </Heading>
                <p className="hero__subtitle">{siteConfig.tagline}</p>
                <p className={styles.heroDescription}>
                    Built on <strong>.NET 10</strong> · SQL-92 · Cost-based Optimizer ·
                    Disk-backed BTree · ACID Transactions · >90% Test Coverage
                </p>
                <div className={styles.buttons}>
                    <Link
                        className={clsx('button button--lg', styles.heroPrimaryButton)}
                        to="/docs/intro">
                        🚀 Get Started
                    </Link>
                    <Link
                        className={clsx('button button--outline button--lg', styles.heroSecondaryButton)}
                        to="/docs/getting-started/quick-start"
                    >
                        ⚡ Quick Start
                    </Link>
                    <a
                        className={clsx('button button--outline button--lg', styles.heroSecondaryButton)}
                        href="https://github.com/schivei/aeternum-db"
                    >
                        ⭐ Star on GitHub
                    </a>
                </div>
                <div className={styles.badges}>
                    <a href="https://github.com/schivei/aeternum-db/blob/main/LICENSE.md">
                        <img src="https://img.shields.io/badge/License-AGPL%203.0-blue.svg" alt="License AGPL 3.0" />
                    </a>
                    <a href="https://github.com/schivei/aeternum-db">
                        <img src="https://img.shields.io/badge/.NET-10.0-purple.svg" alt=".NET 10" />
                    </a>
                    <a href="https://github.com/schivei/aeternum-db">
                        <img src="https://img.shields.io/badge/Language-C%23-239120.svg" alt="C#" />
                    </a>
                    <a href="https://github.com/schivei/aeternum-db">
                        <img src="https://img.shields.io/badge/Coverage-%3E90%25-brightgreen" alt="Coverage >90%" />
                    </a>
                    <a href="https://github.com/schivei/aeternum-db">
                        <img src="https://img.shields.io/badge/Status-Active%20Development-yellow.svg" alt="Status" />
                    </a>
                    <a href="https://github.com/schivei/aeternum-db">
                        <img src="https://img.shields.io/github/stars/schivei/aeternum-db?style=flat&logo=github" alt="GitHub Stars" />
                    </a>
                </div>
            </div>
        </header>
    );
}

const FeatureList = [
    {
        title: 'ACID Transactions',
        emoji: '🔐',
        description: (
            <>
                Full ACID compliance with multiple isolation levels. AeternumDB
                guarantees data integrity and consistency across every operation —
                from a single insert to complex multi-table transactions.
            </>
        ),
    },
    {
        title: 'SQL-92 + Extensions',
        emoji: '📝',
        description: (
            <>
                Write familiar SQL and immediately benefit from AeternumDB's unique
                extensions: vector columns, reference types, temporal tables, EXPAND,
                and VIEW AS.
            </>
        ),
    },
    {
        title: 'Disk-Backed BTree',
        emoji: '🌲',
        description: (
            <>
                Persistent B+ tree with node serialization, sibling links for O(k)
                range scans, crash-safe metadata page, and bulk-load for million-row
                initial population in seconds.
            </>
        ),
    },
    {
        title: 'Cost-Based Optimizer',
        emoji: '⚡',
        description: (
            <>
                Rule-driven optimizer with constant folding, predicate pushdown,
                projection pushdown, and join reordering. Selects the best physical
                operator automatically using statistics and cost estimates.
            </>
        ),
    },
    {
        title: 'Multi-Paradigm',
        emoji: '🔄',
        description: (
            <>
                Relational, document, graph, and JSON in a single engine. No more
                managing a zoo of specialized databases for different data shapes.
            </>
        ),
    },
    {
        title: 'Cloud-Native',
        emoji: '☁️',
        description: (
            <>
                Horizontal sharding and read replica primitives built in from day one.
                Designed for modern distributed architectures, Kubernetes, and
                serverless deployments.
            </>
        ),
    },
    {
        title: '>90% Test Coverage',
        emoji: '🧪',
        description: (
            <>
                More than 180 xUnit tests covering storage, SQL parsing, execution,
                query planning, and BTree operations. Quality is a first-class citizen,
                not an afterthought.
            </>
        ),
    },
    {
        title: 'Memory-Safe C#',
        emoji: '🛡️',
        description: (
            <>
                Written in modern C# (.NET 10) with unsafe code restricted to
                storage/index hot paths. No garbage collection surprises, no
                null-reference nightmares — just predictable, fast execution.
            </>
        ),
    },
    {
        title: 'Open Source (AGPL)',
        emoji: '🌍',
        description: (
            <>
                Community-driven under AGPL-3.0. Every improvement you make comes
                back to the ecosystem. Extensions and drivers ship under MIT and
                Apache 2.0 for maximum flexibility.
            </>
        ),
    },
];

function Feature({ emoji, title, description }) {
    return (
        <div className={clsx('col col--4')}>
            <div className="text--center">
                <div className={styles.featureEmoji}>{emoji}</div>
            </div>
            <div className="text--center padding-horiz--md">
                <Heading as="h3">{title}</Heading>
                <p>{description}</p>
            </div>
        </div>
    );
}

Feature.propTypes = {
    emoji: PropTypes.string.isRequired,
    title: PropTypes.string.isRequired,
    description: PropTypes.node.isRequired,
};

function HomepageFeatures() {
    return (
        <section className={styles.features}>
            <div className="container">
                <Heading as="h2" className="text--center margin-bottom--lg">
                    Why AeternumDB?
                </Heading>
                <div className="row">
                    {FeatureList.map((props) => (
                        <Feature key={props.title} {...props} /> // NOSONAR S6774
                    ))}
                </div>
            </div>
        </section>
    );
}

function StatsSection() {
    return (
        <section className={styles.statsSection}>
            <div className="container">
                <div className="row">
                    {[
                        { stat: '>90%', label: 'Test Coverage' },
                        { stat: '180+', label: 'Unit Tests' },
                        { stat: '5',    label: 'Core Modules' },
                        { stat: 'AGPL', label: 'Open Source' },
                    ].map(({ stat, label }) => (
                        <div key={label} className={clsx('col col--3', styles.statItem)}>
                            <div className={styles.statValue}>{stat}</div>
                            <div className={styles.statLabel}>{label}</div>
                        </div>
                    ))}
                </div>
            </div>
        </section>
    );
}

function QuickExample() {
    return (
        <section className={styles.quickExample}>
            <div className="container">
                <div className="row">
                    <div className="col col--7">
                        <Heading as="h2" className="margin-bottom--lg">
                            ⚡ Zero to Query in Minutes
                        </Heading>
                        <div className="margin-bottom--lg">
                            <pre>
                                <code className="language-csharp">{`// 1. Open (or create) a database file
await using var storage = await StorageEngine.OpenAsync("mydb.adb");

// 2. Create a persistent BTree index
var btree = await BTree.CreateAsync(storage);

// 3. Insert data — async, crash-safe
await btree.InsertAsync(42L, 420UL);
await btree.InsertAsync(99L, 990UL);

// 4. Point lookup — O(log n)
var result = await btree.SearchAsync(42L);
Console.WriteLine(result); // 420

// 5. Range scan — O(log n + k) via sibling links
await foreach (var (k, v) in btree.RangeScanAsync(1L, 100L))
    Console.WriteLine($"Key={k}, Value={v}");

// 6. Plan + EXPLAIN a SQL query
var planner = new QueryPlanner();
var stmt    = new SqlParser().ParseOne(
    "SELECT id, name FROM users WHERE age > 18 ORDER BY name");
Console.WriteLine(planner.Explain(planner.Plan(stmt, context)));`}</code>
                            </pre>
                        </div>
                    </div>
                    <div className={clsx('col col--5', styles.quickLinks)}>
                        <Heading as="h3">📚 Explore the Docs</Heading>
                        <ul className={styles.linkList}>
                            <li>📦 <Link to="/docs/getting-started/installation">Installation Guide</Link></li>
                            <li>⚡ <Link to="/docs/getting-started/quick-start">Quick Start</Link></li>
                            <li>⚙️ <Link to="/docs/getting-started/configuration">Configuration</Link></li>
                            <li>📝 <Link to="/docs/guides/sql-reference">SQL Reference</Link></li>
                            <li>🧠 <Link to="/docs/guides/query-optimization">Query Optimization</Link></li>
                            <li>🔍 <Link to="/docs/guides/explain">EXPLAIN</Link></li>
                            <li>🌲 <Link to="/docs/guides/btree-usage">BTree Index Usage</Link></li>
                            <li>🗄️ <Link to="/docs/architecture/storage">Storage Architecture</Link></li>
                            <li>⚡ <Link to="/docs/architecture/query-optimizer">Optimizer Architecture</Link></li>
                        </ul>
                        <div className={styles.ctaBlock}>
                            <Link className="button button--primary button--lg" to="/docs/intro">
                                Read the Docs →
                            </Link>
                        </div>
                    </div>
                </div>
            </div>
        </section>
    );
}

export default function Home() {
    const { siteConfig } = useDocusaurusContext();
    return (
        <Layout
            title={`${siteConfig.title} - ${siteConfig.tagline}`}
            description="High-Performance, Extensible Database Management System built in C# with SQL support, BTree indexes, and a cost-based query optimizer.">
            <HomepageHeader />
            <main>
                <StatsSection />
                <HomepageFeatures />
                <QuickExample />
            </main>
        </Layout>
    );
}
