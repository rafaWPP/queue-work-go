# Segurança e implantação

Esta branch contém alterações experimentais que precisam ser testadas antes de serem aplicadas ao servidor de produção.

## Credenciais

- Nunca faça commit de `.env`, tokens, chaves ou dados de clientes.
- Um `.env` esteve presente no histórico público do repositório. Considere os segredos antigos comprometidos, revogue e substitua todos os valores reais; excluir o arquivo de um commit recente não limpa o histórico.
- Não exponha o painel diretamente à internet sem HTTPS, autenticação robusta e controle de acesso.

## Autenticação do painel

A branch experimental substituiu `?auth=` por login via POST e cookie de sessão HMAC com expiração de 12 horas. Ações destrutivas exigem POST e validação do cabeçalho `Origin`. O cookie utiliza `HttpOnly` e `SameSite=Strict`; o atributo `Secure` depende da detecção de HTTPS. **Use um proxy reverso confiável que controle `X-Forwarded-Proto` e preserve `Host`, e configure HTTPS.** Não exponha diretamente à internet antes de uma revisão de segurança. O segredo `AUTH` deve ser forte e sua troca invalida todas as sessões. Esta alteração ainda requer validação do workflow e testes de implantação.

## Reserva de tarefas

A reserva transacional usa `SELECT ... FOR UPDATE SKIP LOCKED` e requer MySQL 8+ ou banco compatível. Execute testes de concorrência com múltiplos workers antes de implantar. A reserva atômica evita a seleção simultânea de uma tarefa pendente, mas **não garante processamento exatamente uma vez** diante de falhas após efeitos externos; os jobs devem ser idempotentes.

## Recuperação de tarefas

A rotina que marca tarefas `processing` antigas como falhas pode afetar jobs legítimos de longa duração. Recomenda-se implementar heartbeat, lease por worker e política explícita de retry/dead-letter antes de habilitar recuperação automática em ambientes com jobs longos.

## Licenciamento

O projeto utiliza a licença MIT, conforme o arquivo [LICENSE](LICENSE). Certifique-se de que todos os componentes publicados possam ser distribuídos sob essa licença.
