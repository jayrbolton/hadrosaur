.PHONY: test

test:
	PYTHONPATH=. pytest -s -vv test
