awswrangler_layer_build:
	docker run --rm \
		-v ${CURDIR}/lambda_layers/awswrangler:/var/task \
		lambci/lambda:build-python3.8 \
        curl -OL https://github.com/awslabs/aws-data-wrangler/releases/download/2.12.1/awswrangler-layer-2.12.1-py3.8.zip
#       && unzip awswrangler-layer-2.12.1-py3.8.zip -ｄ python/
