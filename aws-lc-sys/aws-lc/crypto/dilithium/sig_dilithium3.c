// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR ISC
#include "../evp_extra/internal.h"
#include "../fipsmodule/evp/internal.h"
#include "sig_dilithium.h"
#include "pqcrystals_dilithium_ref_common/sign.h"
#include "pqcrystals_dilithium_ref_common/params.h"

// These includes are required to compile ML-DSA. These can be moved to bcm.c
// when ML-DSA is added to the fipsmodule directory.
#include "./pqcrystals_dilithium_ref_common/fips202.c"
#include "./pqcrystals_dilithium_ref_common/ntt.c"
#include "./pqcrystals_dilithium_ref_common/packing.c"
#include "./pqcrystals_dilithium_ref_common/params.c"
#include "./pqcrystals_dilithium_ref_common/poly.c"
#include "./pqcrystals_dilithium_ref_common/polyvec.c"
#include "./pqcrystals_dilithium_ref_common/reduce.c"
#include "./pqcrystals_dilithium_ref_common/rounding.c"
#include "./pqcrystals_dilithium_ref_common/sign.c"
#include "./pqcrystals_dilithium_ref_common/symmetric-shake.c"

// Note: These methods currently default to using the reference code for
// Dilithium. In a future where AWS-LC has optimized options available,
// those can be conditionally (or based on compile-time flags) called here,
// depending on platform support.

int ml_dsa_65_keypair(uint8_t *public_key  /* OUT */,
                       uint8_t *secret_key /* OUT */) {
  ml_dsa_params params;
  ml_dsa_65_params_init(&params);
  return crypto_sign_keypair(&params, public_key, secret_key);
}

int ml_dsa_65_sign(uint8_t *sig                /* OUT */,
                    size_t *sig_len            /* OUT */,
                    const uint8_t *message     /* IN */,
                    size_t message_len         /* IN */,
                    const uint8_t *ctx         /* IN */,
                    size_t ctx_len             /* IN */,
                    const uint8_t *secret_key  /* IN */) {
  ml_dsa_params params;
  ml_dsa_65_params_init(&params);
  return crypto_sign_signature(&params, sig, sig_len, message, message_len,
                                             ctx, ctx_len, secret_key);
}

int ml_dsa_65_verify(const uint8_t *message     /* IN */,
                      size_t message_len        /* IN */,
                      const uint8_t *sig        /* IN */,
                      size_t sig_len            /* IN */,
                      const uint8_t *ctx        /* IN */,
                      size_t ctx_len            /* IN */,
                      const uint8_t *public_key /* IN */) {
  ml_dsa_params params;
  ml_dsa_65_params_init(&params);
  return crypto_sign_verify(&params, sig, sig_len, message, message_len,
                                          ctx, ctx_len, public_key);
}
