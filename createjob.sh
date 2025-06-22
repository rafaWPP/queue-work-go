#!/bin/bash
# createjob.sh - Cria um arquivo de job com template

# Verifica se foi passado o nome do job
if [ "$#" -lt 1 ]; then
    echo "Uso: $0 NomeDoJob"
    exit 1
fi

# O nome do job conforme passado pelo usuário
JOB_INPUT="$1"

# Converte o nome para TitleCase para usar na struct
# Exemplo: "sendmessage" -> "Sendmessage"
JOB_TITLE=$(echo "$JOB_INPUT" | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')

# Converte o nome para minúsculas para o nome do arquivo
JOB_FILE=$(echo "$JOB_INPUT" | tr '[:upper:]' '[:lower:]')

# Define o caminho de destino para o arquivo
OUTPUT_PATH="jobs/${JOB_FILE}.go"

# Cria o diretório "jobs" se não existir
if [ ! -d "jobs" ]; then
    mkdir -p jobs
fi

# Cria o arquivo com o template
cat > "$OUTPUT_PATH" <<EOF
package main

import (
	"fmt"
	"go-queue/common" // IMPORTANTE: use o mesmo caminho definido no go.mod
)

// ${JOB_TITLE}Job implementa a interface common.Job.
type ${JOB_TITLE}Job struct {
	Data []byte
}

// Handle implementa a lógica real do job.
// Aqui você pode realizar a chamada para uma API externa ou outra lógica.
func (j *${JOB_TITLE}Job) Handle() error {
	fmt.Println("Executando ${JOB_TITLE}Job com dados:", string(j.Data))
	// Retorne nil para indicar sucesso ou retorne um erro se algo falhar.
	return nil
}

// New é a função exportada que cria uma instância do job.
// Deve ter exatamente a assinatura: func([]byte) common.Job
func New(data []byte) common.Job {
	return &${JOB_TITLE}Job{Data: data}
}
EOF

echo "Job ${JOB_TITLE} criado com sucesso em ${OUTPUT_PATH}"
