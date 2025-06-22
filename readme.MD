WEBTECH QUEUES
Este projeto é um servidor em Go que gerencia filas (queues) e plugins. O processo de build e deploy é feito via script build.sh e o serviço é gerenciado pelo systemd.

Requisitos
Go instalado (para compilar o projeto)
Instruções
1. Build e Início do Serviço
O script build.sh realiza as seguintes ações:

Limpa e compila os plugins (localizados na pasta jobs/).
Compila o executável principal (webtech-queue).
Copia o arquivo de serviço (go-server.service) para o diretório do systemd (/etc/systemd/system/).
Recarrega o systemd, inicia o serviço e o habilita para iniciar no boot.

Para compilar e iniciar o serviço, execute:

Apenas uma ves para tornar executavel
chmod +x build.sh

Iniciar o sistema
./build.sh

Se tudo estiver configurado corretamente, o serviço será iniciado. Você pode verificar o status do serviço com:

sudo systemctl status go-server.service

2. Removendo o Serviço
O script build.sh também cria um script chamado remove.sh que:

Para o serviço.
Desabilita o serviço para iniciar automaticamente no boot.
Remove o arquivo de serviço copiado em /etc/systemd/system/go-server.service.
Recarrega as configurações do systemd.

Para reverter a instalação (remover o serviço), execute:

./remove.sh

3. Visualizando os Logs
Para acompanhar os logs do serviço em tempo real, use o comando:

sudo journalctl -u go-server.service -f

Esse comando exibirá os logs gerados pelo serviço conforme são produzidos.

Variáveis de Ambiente
As variáveis de ambiente são definidas em um arquivo .env (localizado em .env) ou diretamente no arquivo de serviço. As principais variáveis utilizadas são:

PORT – Porta em que o servidor será executado (ex.: 9090).
AUTH – Chave de autenticação para acesso ao serviço (ex.: webtech).
DB_USER – Usuário do banco de dados.
DB_PASSWORD – Senha do banco de dados.
DB_NAME – Nome do banco de dados.
DB_HOST – Endereço do banco de dados.
DB_PORT – Porta do banco de dados.
Se preferir, você pode utilizar a diretiva EnvironmentFile no arquivo de serviço para carregar essas variáveis de um único arquivo.

Exemplo de Arquivo de Serviço (systemd)
Abaixo um exemplo do arquivo go-server.service:

ini
Copiar
[Unit]
Description=Servidor Go WEBTECH QUEUES
After=network.target

[Service]
ExecStart=/home/go-queue/webtech-queue
WorkingDirectory=/home/go-queue
Restart=always
RestartSec=5
User=wagestor-devnewgestor
Group=wagestor-devnewgestor
EnvironmentFile=/home/go-queue/.env

[Install]
WantedBy=multi-user.target
