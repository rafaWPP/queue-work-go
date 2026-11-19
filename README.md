# Queue Work Go

Um processador de tarefas em segundo plano escrito em Go, com filas persistentes em banco de dados, plugins compilados, agendamento e painel de acompanhamento.

> **Status:** projeto em desenvolvimento. A branch `melhorias-seguranca-filas` contém mudanças experimentais que ainda precisam de testes de integração. Não implante esta branch diretamente em produção.

## Para que serve

O projeto separa tarefas demoradas do fluxo principal de uma aplicação. Exemplos de tarefas incluem notificações, integrações HTTP e processamento assíncrono. O mecanismo central armazena tarefas em `queue_tasks`, seleciona jobs elegíveis e carrega implementações da interface `common.Job` como plugins Go.

## Recursos existentes

- Filas nomeadas e persistidas em banco de dados
- Jobs implementados como plugins Go (`.so`)
- Agendamento por `available_at`
- Estados `pending`, `processing`, `completed` e `failed`
- Painel web com tarefas, falhas e histórico
- Migrações SQL e scripts de build/systemd
- Exemplos de integrações específicas em `jobs/`

## Requisitos

- Go conforme `go.mod`
- Banco MySQL/MariaDB compatível com as migrações
- Linux para o fluxo atual de plugins e systemd
- **Para a branch experimental:** suporte a `SELECT ... FOR UPDATE SKIP LOCKED`. Verifique a versão exata do seu MariaDB antes de usar; não há matriz de compatibilidade testada.

## Visão geral do fluxo

1. Uma aplicação insere uma tarefa na tabela `queue_tasks`.
2. Um worker reserva a tarefa elegível para execução.
3. O worker carrega o plugin correspondente ao `task_name`.
4. O plugin implementa `Handle() error`.
5. O resultado é registrado como sucesso ou falha.

Exemplo ilustrativo de inserção (requer schema já migrado e plugin compilado):

```sql
INSERT INTO queue_tasks (queue_name, task_name, data, status)
VALUES ('default', 'jobs/example', '{"message":"hello"}', 'pending');
```

O caminho do plugin acima é apenas ilustrativo; crie e compile `jobs/example.go` antes de executá-lo.

## Desenvolvimento local

```bash
go mod download
go test ./...
go build ./...
```

O build dos plugins depende da plataforma e de uma versão compatível do compilador Go. Consulte `build.sh` e `cmd/webtech/main.go` antes de usar o script de instalação, pois ele interage com o systemd.

## Configuração

Use variáveis de ambiente para `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `PORT` e `AUTH`. Nunca faça commit de arquivos `.env` reais. Consulte `.env.example`.

**Aviso de segurança:** a interface web atual ainda aceita `?auth=` nas URLs, o que pode expor a chave em históricos e logs. Não exponha o painel diretamente à internet. Há também uma rotina de recuperação de jobs que precisa de testes com tarefas de longa duração. Veja `SECURITY.md`.

## Estrutura

- `main.go`: ciclo de vida do worker, reserva de tarefas e migrações
- `common/`: interface e utilitários de jobs
- `jobs/`: integrações específicas e exemplos de plugins
- `migrations/`: estrutura do banco de dados
- `web/`: painel de acompanhamento
- `cmd/webtech/`: comandos de desenvolvimento
- `build.sh`: build e instalação do serviço

## Estado do projeto e roadmap

- [ ] Testes automatizados de concorrência e integração com MariaDB
- [ ] Sessões seguras no painel, sem token em URL
- [ ] Política documentada de retry, heartbeat e idempotência
- [ ] Separar o motor genérico das integrações específicas de cada produto
- [ ] Exemplos independentes de sistemas comerciais
- [ ] Benchmarks reproduzíveis e releases versionadas
- [x] Adicionar licença MIT

## Contribuições

Issues, discussões técnicas e pull requests são bem-vindos. Veja `CONTRIBUTING.md` e `SECURITY.md`.

## Licença

Licenciado sob a **MIT License**. Consulte o arquivo [LICENSE](LICENSE).
