🤝 Contribuindo para o AeternumDB

Obrigado por considerar contribuir para o AeternumDB! Este projeto é ambicioso e depende da colaboração da comunidade para evoluir.

Este documento descreve como você pode participar, quais áreas estão abertas a contribuições e como o licenciamento se aplica a cada parte do projeto.

---

📂 Estrutura do Projeto

O repositório é organizado em módulos principais:

• core/ → Engine principal em Rust (AGPLv3.0)
• extensions/ → Plugins WASM (MIT)
• drivers/ → ODBC, JDBC, binário, gRPC (Apache 2.0)
• sdks/ → SDKs em Rust, C++, Python, JS/TS, Go, Java/Kotlin, .NET Core (Apache 2.0)
• deployment/ → Configurações Lite, Container, Serverless
• docs/ → Documentação, whitepapers
• tests/ → Testes unitários e de integração


---

📜 Licenciamento e Áreas de Contribuição

• Core Engine (AGPLv3.0)• Contribuições devem ser compatíveis com AGPLv3.0.
• Qualquer modificação ou derivado precisa manter a mesma licença.
• Ideal para quem deseja trabalhar em ACID, replicação, sharding, versionamento e segurança.

• SDKs e Drivers (Apache 2.0)• Contribuições podem ser usadas em ambientes comerciais sem obrigação de abrir código.
• Ideal para quem deseja criar integrações em diferentes linguagens.

• Extensões WASM (MIT)• Contribuições são totalmente livres e flexíveis.
• Ideal para quem deseja criar novos paradigmas de dados, funções customizadas ou conectores.



---

🛠️ Como Contribuir

1. Fork o repositório e crie sua branch (feature/nome-da-feature).
2. Siga os padrões de código definidos em cada módulo (Rust para core, linguagem correspondente para SDKs).
3. Adicione testes sempre que possível.
4. Abra um Pull Request (PR) descrevendo claramente:• O problema ou melhoria que está sendo resolvido.
• A área do projeto afetada (core, SDK, extensão, driver).
• Qual licença se aplica à sua contribuição.



---

✅ Boas Práticas

• Escreva código limpo e documentado.
• Mantenha commits pequenos e descritivos.
• Use testes automatizados para validar suas mudanças.
• Respeite o CODE_OF_CONDUCT.md.


---

🌍 Comunidade

• Discussões e ideias podem ser abertas via Issues.
• Extensões e SDKs podem ser mantidos em sub-repositórios no futuro.
• Incentivamos contribuições tanto técnicas quanto de documentação.


---

💡 Futuro

O projeto prevê uma versão Enterprise Edition com licença comercial. Contribuições para essa versão serão discutidas separadamente, mas a comunidade sempre terá acesso ao núcleo AGPLv3.0 e às extensões open source.
