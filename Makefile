SERVICE_NAME				:= count
PKG            			:= github.com/NTSka/row-count
INPUT_DIRECTORY := input
INPUT_FILE := test-input.log
TEST_INPUT := test-input-test.log
OUTPUT_DIRECTORY := output
OUTPUT_FILE := test-output.log
TEMP_DIRECTORY := temp

build: ## Build the executable file of service.
	echo "Building..."
	cd cmd/${SERVICE_NAME} && go build

run: build ## Run service with local config.
	echo "Running..."
	cd cmd/${SERVICE_NAME} && ./$(SERVICE_NAME) --input=../../${INPUT_DIRECTORY}/${INPUT_FILE} --limit=150 --output=../../${OUTPUT_DIRECTORY}/${OUTPUT_FILE} --temp=../../${TEMP_DIRECTORY}

test: build ## Run service with local config.
	echo "Running..."
	cd cmd/${SERVICE_NAME} && ./$(SERVICE_NAME) --input=../../${INPUT_DIRECTORY}/${TEST_INPUT} --limit=3 --output=../../${OUTPUT_DIRECTORY}/${OUTPUT_FILE} --temp=../../${TEMP_DIRECTORY}
