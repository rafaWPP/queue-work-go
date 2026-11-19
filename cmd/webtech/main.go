package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Nome do diretório de jobs
const jobsDir = "jobs"

// Template do código base para novos jobs
const jobTemplate = `package main

import (
	"fmt"
	"go-queue/common" // Use o caminho correto do módulo
)

// %[1]sJob implementa a interface common.Job.
type %[1]sJob struct {
	Data []byte
}

// Handle executa a lógica do job.
func (j *%[1]sJob) Handle() error {
	fmt.Println("Executando %[1]sJob com dados:", string(j.Data))
	return nil
}

// New é a função exportada para criar o job.
func New(data []byte) common.Job {
	return &%[1]sJob{Data: data}
}
`

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: webtech <comando> [opções]")
		fmt.Println("Comandos disponíveis:")
		fmt.Println("  create:job <NomeDoJob>  - Cria um novo job")
		fmt.Println("  build                   - Compila e executa os plugins e o sistema principal")
		fmt.Println("  clean                   - Remove arquivos compilados (.so, binários) e recompila o CLI")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "create:job":
		if len(os.Args) < 3 {
			fmt.Println("Erro: Você deve fornecer o nome do job. Exemplo: webtech create:job SendMessage")
			os.Exit(1)
		}
		createJob(os.Args[2])

	case "build":
		buildAndRun()

	case "clean":
		cleanCompiled()

	default:
		fmt.Println("Comando desconhecido:", os.Args[1])
		os.Exit(1)
	}
}

// createJob cria um novo job no diretório "jobs".
func createJob(jobName string) {
	// Garante que o nome do job comece com letra maiúscula
	jobTitle := strings.Title(strings.ToLower(jobName))
	jobFileName := strings.ToLower(jobName) + ".go"
	jobFilePath := filepath.Join(jobsDir, jobFileName)

	// Verifica se o job já existe
	if _, err := os.Stat(jobFilePath); err == nil {
		fmt.Println("Erro: O job já existe:", jobFilePath)
		os.Exit(1)
	}

	// Cria o diretório "jobs" caso não exista
	if _, err := os.Stat(jobsDir); os.IsNotExist(err) {
		os.Mkdir(jobsDir, 0755)
	}

	// Gera o código do novo job
	content := fmt.Sprintf(jobTemplate, jobTitle)

	// Escreve o novo job no arquivo
	err := os.WriteFile(jobFilePath, []byte(content), 0644)
	if err != nil {
		fmt.Println("Erro ao criar o job:", err)
		os.Exit(1)
	}

	fmt.Println("Job criado com sucesso:", jobFilePath)
}

// buildAndRun compila os plugins e inicia o sistema principal.
func buildAndRun() {
	fmt.Println("Compilando plugins...")

	// Primeiro, limpamos qualquer compilação antiga.
	cleanCompiled()

	// Lê o diretório de jobs (somente a raiz do jobs/)
	files, err := os.ReadDir(jobsDir)
	if err != nil {
		fmt.Println("Erro ao acessar o diretório de jobs:", err)
		os.Exit(1)
	}

	// Compila cada arquivo .go como plugin
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".go") {
			jobName := strings.TrimSuffix(file.Name(), ".go")
			soFile := filepath.Join(jobsDir, jobName+".so")

			fmt.Println("Compilando:", file.Name(), "->", soFile)
			cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", soFile, filepath.Join(jobsDir, file.Name()))
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				fmt.Println("Erro ao compilar plugin:", file.Name(), "-", err)
				os.Exit(1)
			}
		}
	}

	// Compila plugins em jobs/mk (se existir)
	mkDir := filepath.Join(jobsDir, "mk")
	if entries, err := os.ReadDir(mkDir); err == nil {
		fmt.Println("Compilando plugins mk...")
		for _, file := range entries {
			if strings.HasSuffix(file.Name(), ".go") {
				jobName := strings.TrimSuffix(file.Name(), ".go")
				soFile := filepath.Join(mkDir, jobName+".so")
				fmt.Println("Compilando:", file.Name(), "->", soFile)
				cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", soFile, filepath.Join(mkDir, file.Name()))
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				if err := cmd.Run(); err != nil {
					fmt.Println("Erro ao compilar plugin mk:", file.Name(), "-", err)
					os.Exit(1)
				}
			}
		}
	}

	fmt.Println("Compilando o sistema principal (main)...")
	cmd := exec.Command("go", "build", "-o", "main")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Println("Erro ao compilar o sistema principal:", err)
		os.Exit(1)
	}

	// Inicia o sistema principal
	fmt.Println("Iniciando o sistema principal (./main)...")
	startCmd := exec.Command("./main")
	startCmd.Stdout = os.Stdout
	startCmd.Stderr = os.Stderr
	if err := startCmd.Run(); err != nil {
		fmt.Println("Erro ao iniciar o sistema principal:", err)
	}
}

// cleanCompiled remove os arquivos compilados (.so e binários) e recompila o CLI.
func cleanCompiled() {
	fmt.Println("Limpando arquivos compilados...")

	// Remove recursivamente todos os .so em jobs/
	err := filepath.WalkDir(jobsDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".so") {
			fmt.Println("Removendo:", path)
			if err := os.Remove(path); err != nil {
				fmt.Println("Erro ao remover", path, ":", err)
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println("Erro ao varrer 'jobs':", err)
	}

	// Remove o binário "main"
	if _, err := os.Stat("main"); err == nil {
		fmt.Println("Removendo o binário main")
		if err := os.Remove("main"); err != nil {
			fmt.Println("Erro ao remover main:", err)
		}
	}

	// Remove o binário "webtech" e recompila
	removeAndRebuildWebtech()

	fmt.Println("Limpeza concluída.")
}

// removeAndRebuildWebtech remove o binário "webtech" atual e recompila
// usando o arquivo cmd/webtech/main.go.
func removeAndRebuildWebtech() {
	const cliBin = "webtech"

	// Se existir "webtech", removemos
	if _, err := os.Stat(cliBin); err == nil {
		fmt.Println("Removendo o CLI:", cliBin)
		if rmErr := os.Remove(cliBin); rmErr != nil {
			fmt.Println("Erro ao remover webtech:", rmErr)
			return
		}
	}

	// Reconstruir o CLI a partir de cmd/webtech/main.go
	fmt.Println("Recompilando o CLI a partir de cmd/webtech/main.go...")
	cmd := exec.Command("go", "build", "-o", cliBin, "cmd/webtech/main.go")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Println("Erro ao recompilar webtech CLI:", err)
	} else {
		fmt.Println("CLI 'webtech' recompilado com sucesso!")
	}
}
