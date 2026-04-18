# Makefile
SGX_SIGNER_KEY ?= $(HOME)/.config/gramine/enclave-key.pem

all: kafka.manifest.sgx

kafka.manifest: kafka.manifest.template
	gramine-manifest $< $@

kafka.manifest.sgx: kafka.manifest
	gramine-sgx-sign --key $(SGX_SIGNER_KEY) --manifest $< --output $@
clean:
	rm -f kafka.manifest kafka.manifest.sgx kafka.sig
