# Como contribuir

Obrigado por considerar contribuir com o Queue Work Go.

## Antes de começar

- Abra uma issue descrevendo o problema ou a melhoria proposta.
- Evite enviar dados de clientes, arquivos `.env`, chaves, tokens ou logs de produção.
- Para correções, explique como reproduzir o problema e qual comportamento é esperado.
- Para alterações no mecanismo de filas, inclua cenários de concorrência, falhas e reinicialização.

## Desenvolvimento

```bash
go mod download
go test ./...
go build ./...
```

O projeto utiliza plugins Go, que possuem restrições de plataforma e compatibilidade de compilação. As rotinas de integração dependem de serviços externos; não execute os jobs reais sem um ambiente de testes isolado.

## Pull requests

1. Crie uma branch para sua alteração.
2. Mantenha os commits focados.
3. Adicione ou atualize testes quando possível.
4. Atualize a documentação de comportamento ou configuração.
5. Explique riscos de compatibilidade com MySQL/MariaDB.

Não envie vulnerabilidades exploráveis em issues públicas. Consulte `SECURITY.md`.

## Código de conduta

Mantenha as discussões técnicas respeitosas e centradas no problema. O mantenedor revisará contribuições conforme disponibilidade.
