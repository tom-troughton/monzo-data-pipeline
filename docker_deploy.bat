@echo off
setlocal

:: Configuration variables - sample values
set AWS_ACCOUNT=123
set AWS_REGION=xx-xxxx-x
set IMAGE_NAME=image-name
set ECR_REPO=%AWS_ACCOUNT%.dkr.ecr.%AWS_REGION%.amazonaws.com
set TAG=image-tag

echo Building Docker image...
docker build -t %IMAGE_NAME%:%TAG% .

echo Tagging image for ECR...
docker tag %IMAGE_NAME%:%TAG% %ECR_REPO%/%IMAGE_NAME%:%TAG%

echo Logging into ECR...
aws ecr get-login-password --region %AWS_REGION% | docker login --username AWS --password-stdin %ECR_REPO%

echo Pushing image to ECR...
docker push %ECR_REPO%/%IMAGE_NAME%:%TAG%

echo Deploy completed.

endlocal