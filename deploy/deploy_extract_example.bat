:: Creates image from lambdas/extract, uploads to AWS ECR, then updates the AWS lambda function with the new image

@echo off
setlocal

:: Changing directory to project root
cd /d %~dp0\..

:: Getting current date for image tag
for /f "tokens=2 delims==" %%a in ('wmic os get localdatetime /value') do set ldt=%%a

:: Configuration variables
set currentDate=%ldt:~0,8%
set AWS_ACCOUNT=123123123123
set AWS_REGION=xx-xxxx-x
set REPOSITORY_NAME=ecr-repo-name
set ECR_REPO_ROOT=%AWS_ACCOUNT%.dkr.ecr.%AWS_REGION%.amazonaws.com
set ECR_REPO=%ECR_REPO_ROOT%/%REPOSITORY_NAME%
set TAG=extract-%currentDate%
set LAMBDA_FUNCTION_NAME=lambda-function-name

echo Building Docker image...
docker build -f docker/lambda-extract/Dockerfile -t %REPOSITORY_NAME%:%TAG% .

echo Tagging image for ECR...
docker tag %REPOSITORY_NAME%:%TAG% %ECR_REPO%:%TAG%

echo Logging into ECR...
aws ecr get-login-password --region %AWS_REGION% | docker login --username AWS --password-stdin %ECR_REPO%

echo Pushing image to ECR...
docker push %ECR_REPO%:%TAG%

echo Updating the Lambda function...
aws lambda update-function-code --function-name %LAMBDA_FUNCTION_NAME% --image-uri %ECR_REPO%:%TAG%

echo Deploy finished

endlocal

pause