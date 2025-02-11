# Dockerfile-load-staging
FROM public.ecr.aws/lambda/python:3.12

# Create app directory
WORKDIR /var/task

# Copy entire project
COPY lambdas/load_staging/ ./lambdas/load_staging/

# Install the shared library as well as any local requirements
RUN pip install -r lambdas/load_staging/requirements.txt

# Set the CMD to your handler
CMD [ "lambdas/load_staging/main.lambda_handler" ]
