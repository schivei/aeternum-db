🗺️ Roadmap – AeternumDB

Este documento descreve as fases de desenvolvimento e os principais marcos do AeternumDB. O objetivo é fornecer uma visão clara da evolução do projeto, desde o núcleo inicial até versões distribuídas e serverless.

---

📌 Fase 1 – Núcleo Básico

Objetivo: Criar o motor fundamental do banco de dados.

• Implementar engine ACID em Rust
• Suporte inicial a SQL-like queries
• Decimal Engine para precisão numérica
• JSON/JSON2 com schema fixo
• Versionamento básico de tabelas/linhas
• Testes unitários e integração inicial


---

📌 Fase 2 – Extensibilidade

Objetivo: Permitir plugins e extensões.

• Sistema de extensões via WASM
• Extensão de exemplo: GraphQL Engine
• Extensão de exemplo: Object Layer (OO)
• Procedural Languages (Python, JS, .NET Core)
• Documentação para criação de plugins


---

📌 Fase 3 – Distribuição e Escalabilidade

Objetivo: Tornar o banco distribuído e escalável.

• Replicação nativa
• Sharding por linhas e colunas
• Cluster distribuído com tolerância a falhas
• Comunicação via gRPC e protocolo binário nativo
• Observabilidade (monitoramento e métricas)


---

📌 Fase 4 – Drivers e SDKs

Objetivo: Ampliar integração com diferentes linguagens e plataformas.

• Drivers ODBC (32/64 bits, multiplataforma)
• Driver JDBC
• SDK Rust
• SDK Python
• SDK Java/Kotlin
• SDK .NET Core
• SDK Go
• SDK JS/TS


---

📌 Fase 5 – Ambientes de Execução

Objetivo: Suporte a diferentes cenários de implantação.

• Versão Lite (instância única local)
• Versão Containerizada (Docker/Kubernetes)
• Versão Serverless (AWS Lambda, Azure Functions, GCP Cloud Run)
• Configuração multi-cloud para escalabilidade sob demanda


---

📌 Fase 6 – Segurança e Compliance

Objetivo: Garantir confiabilidade e conformidade.

• Criptografia em trânsito e em repouso
• Autenticação forte (OAuth2, JWT)
• Auditoria avançada via extensões
• Ferramentas de conformidade (LGPD, GDPR)


---

📌 Fase 7 – Enterprise Edition

Objetivo: Criar versão comercial para monetização.

• Recursos avançados exclusivos
• SLA e suporte dedicado
• Licenciamento alternativo para empresas
• Ferramentas de administração corporativa


---

📖 Notas

• O roadmap é iterativo: fases podem evoluir em paralelo.
• Cada milestone deve ser acompanhado de documentação e testes.
• A comunidade pode propor novas fases e extensões via Issues.
