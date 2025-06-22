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
