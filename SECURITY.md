# Segurança e implantação

Esta branch contém alterações experimentais que precisam ser testadas antes de serem aplicadas ao servidor de produção.

## Credenciais

- Nunca faça commit de `.env`, tokens, chaves ou dados de clientes.
- Um `.env` esteve presente no histórico público do repositório. Considere os segredos antigos comprometidos, revogue e substitua todos os valores reais; excluir o arquivo de um commit recente não limpa o histórico.
- Não exponha o painel diretamente à internet sem HTTPS, autenticação robusta e controle de acesso.

## Autenticação do painel

O painel ainda aceita `?auth=` por compatibilidade com o sistema existente. **Isso não é uma solução de autenticação segura**: a chave pode aparecer em logs, histórico e URLs copiadas. O próximo passo é implementar sessão via cookie `HttpOnly`, `Secure`, `SameSite` e proteção CSRF para ações destrutivas, removendo a chave da URL.

## Reserva de tarefas

A reserva transacional usa `SELECT ... FOR UPDATE SKIP LOCKED` e requer MySQL 8+ ou banco compatível. Execute testes de concorrência com múltiplos workers antes de implantar. A reserva atômica evita a seleção simultânea de uma tarefa pendente, mas **não garante processamento exatamente uma vez** diante de falhas após efeitos externos; os jobs devem ser idempotentes.

## Recuperação de tarefas

A rotina que marca tarefas `processing` antigas como falhas pode afetar jobs legítimos de longa duração. Recomenda-se implementar heartbeat, lease por worker e política explícita de retry/dead-letter antes de habilitar recuperação automática em ambientes com jobs longos.

## Licenciamento

O repositório ainda precisa de uma licença open source escolhida pelo proprietário. Não foi adicionada uma licença automaticamente, pois a escolha determina direitos e obrigações de terceiros.
