ROOT_DIR 	:= $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

format:
	# Sort
	cd ${ROOT_DIR}; isort feast_hive/

	# Format
	cd ${ROOT_DIR}; black --target-version py37 feast_hive tests

lint:
	cd ${ROOT_DIR}; mypy feast_hive/ tests/
	cd ${ROOT_DIR}; isort feast_hive/ tests/ --check-only
	cd ${ROOT_DIR}; flake8 feast_hive/ tests/
	cd ${ROOT_DIR}; black --check feast_hive tests

test:
	FEAST_HIVE_HOST=localhost FEAST_HIVE_PORT=10000 pytest --verbose --color=yes tests