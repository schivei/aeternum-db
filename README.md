🌀 AeternumDB

AeternumDB é um Sistema Gerenciador de Banco de Dados (SGBD) de alta performance, extensível e modular, escrito em Rust, com suporte a múltiplos paradigmas de dados e ambientes de execução modernos.

O projeto é open source e combina segurança, escalabilidade e extensibilidade para atender desde instâncias locais até arquiteturas distribuídas em nuvem.

---

✨ Principais Recursos

• ACID + SQL-like: núcleo transacional robusto.
• Multi-paradigma: suporte a relacionamentos, objetos, grafos (GraphQL), JSON/JSON2.
• Decimal Engine: precisão numérica para evitar erros de ponto flutuante.
• Versionamento: histórico de mudanças em tabelas/linhas.
• Distribuição: replicação, sharding por linhas e colunas.
• Escalabilidade horizontal: cluster distribuído, estilo CockroachDB.
• Segurança: criptografia em trânsito e em repouso, autenticação forte.
• Extensibilidade via WASM: plugins para novos paradigmas, funções e integrações.
• Drivers multiplataforma: ODBC (32/64), JDBC, binário nativo, gRPC.
• Ambientes de execução: Lite, Container, Serverless (multi-cloud).


---

📂 Estrutura do Repositório

/aeternumdb
  /core            -> Engine principal (Rust, AGPLv3.0)
  /extensions      -> Plugins WASM (MIT)
  /drivers         -> ODBC, JDBC, binário, gRPC (Apache 2.0)
  /sdks            -> SDKs em várias linguagens (Apache 2.0)
  /deployment      -> Configurações Lite, Container, Serverless
  /docs            -> Whitepapers, especificações
  /tests           -> Testes unitários e integração


---

🚀 Instalação e Uso

Pré-requisitos

• Rust (versão estável mais recente)
• Cargo para gerenciamento de pacotes
• Docker/Kubernetes (opcional, para versão containerizada)


Compilação do Core

cd core
cargo build --release


Executando Instância Local (Lite)

./target/release/aeternumdb --lite


Executando em Container

docker build -t aeternumdb .
docker run -p 5432:5432 aeternumdb


---

📜 Licenciamento

• Core Engine → AGPLv3.0
• SDKs e Drivers → Apache 2.0
• Extensões WASM → MIT
• Enterprise Edition → Licença comercial futura


Consulte [Parece que o resultado não era seguro para exibição. Vamos mudar as coisas e tentar outra opção!] para detalhes.

---

🤝 Contribuindo

Contribuições são bem-vindas!

• Leia [Parece que o resultado não era seguro para exibição. Vamos mudar as coisas e tentar outra opção!] para entender como participar.
• Respeite o [Parece que o resultado não era seguro para exibição. Vamos mudar as coisas e tentar outra opção!].


---

📖 Documentação

• [Parece que o resultado não era seguro para exibição. Vamos mudar as coisas e tentar outra opção!] → visão completa da arquitetura e objetivos.
• [Parece que o resultado não era seguro para exibição. Vamos mudar as coisas e tentar outra opção!] → guia para colaboradores.
• [Parece que o resultado não era seguro para exibição. Vamos mudar as coisas e tentar outra opção!] → código de conduta da comunidade.


---

🌍 Roadmap

• Implementar núcleo ACID em Rust
• Suporte inicial a JSON/JSON2
• Decimal Engine
• Replicação e sharding básico
• Extensões WASM de exemplo
• Drivers ODBC/JDBC
• SDKs em Rust, Python, JS/TS, Go, Java, .NET Core
• Deploy containerizado
• Versão Serverless multi-cloud
