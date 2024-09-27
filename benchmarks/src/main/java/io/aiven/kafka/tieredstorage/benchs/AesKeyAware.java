/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.benchs;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

public class AesKeyAware {
    protected static int ivSize;
    protected static SecretKeySpec secretKey;
    protected static byte[] aad;

    public static void initCrypto() {
        // These are tests, we don't need a secure source of randomness.
        final Random random = new Random();

        final byte[] dataKey = new byte[32];
        random.nextBytes(dataKey);
        secretKey = new SecretKeySpec(dataKey, "AES");

        aad = new byte[32];
        random.nextBytes(aad);

        ivSize = encryptionCipherSupplier().getIV().length;
    }

    protected static Cipher encryptionCipherSupplier() {
        try {
            final Cipher encryptCipher = getCipher();
            encryptCipher.init(Cipher.ENCRYPT_MODE, secretKey, SecureRandom.getInstanceStrong());
            encryptCipher.updateAAD(aad);
            return encryptCipher;
        } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    protected static Cipher decryptionCipherSupplier(final byte[] encryptedChunk) {
        try {
            final Cipher encryptCipher = getCipher();
            encryptCipher.init(Cipher.DECRYPT_MODE, secretKey,
                new GCMParameterSpec(128, encryptedChunk, 0, ivSize),
                SecureRandom.getInstanceStrong());
            encryptCipher.updateAAD(aad);
            return encryptCipher;
        } catch (final NoSuchAlgorithmException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }
    }

    protected static Cipher getCipher() {
        try {
            return Cipher.getInstance("AES/GCM/NoPadding");
        } catch (final NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new RuntimeException(e);
        }
    }
}
