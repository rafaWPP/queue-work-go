# Queue Work Go Standalone — plano de implementação

**Status: arquitetura inicial, NÃO funcional em produção.** Nenhum backend standalone foi ativado. A implementação MariaDB existente continua sendo o caminho atual.

## Objetivo

Executar um motor de filas com um binário Go e persistência local, sem depender de MariaDB, RabbitMQ ou Redis. O MariaDB permanecerá como backend opcional e compatível.

## Estratégia

1. **Contrato de armazenamento:** `internal/queueengine/store.go` define tarefas, leases e operações com token de propriedade.
2. **Backend local:** implementar SQLite embarcado com transações e índices, sem serviço de banco separado. A escolha do driver será avaliada considerando licença, compatibilidade de Go e builds Linux.
3. **Testes de durabilidade:** reiniciar o processo entre enqueue/claim/complete, testar queda durante escrita, concorrência e reentrega após lease expirado.
4. **Integração opt-in:** introduzir configuração explícita de backend, sem mudar o padrão MariaDB nem migrar dados automaticamente.
5. **Observabilidade:** integrar painel e métricas ao contrato de armazenamento.
6. **Escala distribuída:** desenhar coordenação de nós e replicação separadamente; arquivo SQLite local **não** deve ser compartilhado entre servidores via filesystem de rede.

## Invariantes

- Uma tarefa confirmada como persistida deve sobreviver a reinicialização normal.
- Claim e gravação de token devem ser atômicos.
- Um worker com token antigo não pode renovar, concluir nem arquivar uma tarefa reclamada por outro.
- Expiração de lease permite reentrega: **não garante execução exatamente uma vez**. Integrações externas precisam de idempotência.
- Nenhuma migração de banco de produção ou mudança de backend acontece automaticamente.
- Migração `002_add_processing_token.sql` permanece bloqueada na rotina automática até a implementação integral de fencing e aprovação de rollout.

## Critérios antes de oferecer modo standalone

- Testes automatizados de concorrência, recuperação e durabilidade aprovados.
- Benchmark reproduzível de throughput, latência e consumo de memória.
- Política de backup, recuperação e compactação documentada.
- Revisão de segurança e compatibilidade com jobs/plugins existentes.
