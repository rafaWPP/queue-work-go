#!/bin/bash

# Definir cores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # Sem cor

# Limpar todos os plugins
echo -e "${YELLOW}Limpando plugins...${NC}"
./webtech clean
echo -e "${YELLOW}____________________________________________${NC}"

# Compilar todos os plugins em jobs/
echo -e "${GREEN}Compilando plugins...${NC}"
for file in jobs/*.go; do
    base=$(basename "$file" .go)
    echo -e "${YELLOW}Compilando $file em jobs/${base}.so...${NC}"
    go build -buildmode=plugin -o "jobs/${base}.so" "$file"
    if [ $? -ne 0 ]; then
        echo -e "${RED}Erro na compilação do plugin: $file${NC}"
        exit 1
    fi
done

# Compilar todos os plugins em jobs/mk
echo -e "${GREEN}Compilando plugins mk...${NC}"
for file in jobs/mk/*.go; do
    base=$(basename "$file" .go)
    echo -e "${YELLOW}Compilando $file em jobs/mk/${base}.so...${NC}"
    go build -buildmode=plugin -o "jobs/mk/${base}.so" "$file"
    if [ $? -ne 0 ]; then
        echo -e "${RED}Erro na compilação do plugin: $file${NC}"
        exit 1
    fi
done

# Compilar o executável principal
echo -e "${GREEN}Compilando o executável principal...${NC}"
go build -o webtech-queue .
if [ $? -ne 0 ]; then
    echo -e "${RED}Erro na compilação do main${NC}"
    exit 1
fi

# Copiar o arquivo de serviço para o diretório do systemd
echo -e "${GREEN}Copiando o arquivo de serviço para /etc/systemd/system/...${NC}"
sudo cp go-server.service /etc/systemd/system/

# Recarregar o systemd e iniciar o serviço
echo -e "${GREEN}Recarregando systemd e iniciando o serviço...${NC}"
sudo systemctl daemon-reload
sudo systemctl start go-server.service

# Verificar status do serviço
sudo systemctl status go-server.service --no-pager

# Habilitar o serviço para iniciar no boot
sudo systemctl enable go-server.service

# Cria o script remove.sh para reverter a instalação do serviço
cat << 'EOF' > remove.sh
#!/bin/bash
echo "Parando o serviço go-server.service..."
sudo systemctl stop go-server.service

echo "Desabilitando o serviço go-server.service..."
sudo systemctl disable go-server.service

echo "Removendo o arquivo de serviço em /etc/systemd/system/go-server.service..."
sudo rm -f /etc/systemd/system/go-server.service

echo "Recarregando o systemd..."
sudo systemctl daemon-reload

echo "Serviço go-server.service removido com sucesso."
EOF

# Tornar o remove.sh executável
chmod +x remove.sh

echo -e "${GREEN}Script remove.sh criado. Para reverter a instalação, execute: ./remove.sh${NC}"
