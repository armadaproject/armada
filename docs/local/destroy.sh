#!/bin/sh

printf "\n*******************************************************\n"
printf "Destroying Armada server cluster"
printf "\n*******************************************************\n"
kind delete cluster --name quickstart-armada-server
printf "\033[1mdone\033[0m"
printf "\n*******************************************************\n"
printf "Destroying first Armada executor cluster"
printf "\n*******************************************************\n"
kind delete cluster --name quickstart-armada-executor-0
printf "\033[1mdone\033[0m"
printf "\n*******************************************************\n"
printf "Destroying second Armada executor cluster"
printf "\n*******************************************************\n"
kind delete cluster --name quickstart-armada-executor-1
printf "\033[1mdone\033[0m\n"