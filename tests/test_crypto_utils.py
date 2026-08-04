"""
Testes da camada de cifra.

O ponto central é a distinção entre as duas camadas:
- `aes_encrypt` transporta a chave consigo (ofuscação, compat frontend)
- `secret_encrypt` exige a ENCRYPTION_KEY (proteção real em repouso)

O teste `test_chave_errada_nao_decifra` é o que documenta a diferença — foi
precisamente essa propriedade que faltava antes desta alteração.
"""

import pytest

from app.services.crypto_utils import (
    aes_decrypt,
    aes_encrypt,
    generate_master_key,
    is_encrypted_at_rest,
    reencrypt_at_rest,
    secret_decrypt,
    secret_encrypt,
    to_wire,
)

SEGREDO = "P@ssw0rd-da-BD-do-cliente!"


class TestCifraEmRepouso:
    def test_round_trip(self):
        token = secret_encrypt(SEGREDO)

        assert token.startswith("v2.")
        assert secret_decrypt(token) == SEGREDO

    def test_texto_em_claro_nao_aparece_no_token(self):
        assert SEGREDO not in secret_encrypt(SEGREDO)

    def test_nao_deterministico(self):
        # Nonce novo a cada cifra: dois segredos iguais não geram o mesmo
        # token, o que impede correlacionar utilizadores com a mesma password.
        assert secret_encrypt(SEGREDO) != secret_encrypt(SEGREDO)

    def test_chave_errada_nao_decifra(self, chave_mestra_nova):
        token = secret_encrypt(SEGREDO)

        chave_mestra_nova()

        with pytest.raises(ValueError, match="ENCRYPTION_KEY"):
            secret_decrypt(token)

    def test_token_adulterado_e_rejeitado(self):
        token = secret_encrypt(SEGREDO)
        # GCM é autenticado: mexer num byte invalida a tag.
        adulterado = token[:-4] + ("AAAA" if not token.endswith("AAAA") else "BBBB")

        with pytest.raises(ValueError):
            secret_decrypt(adulterado)

    @pytest.mark.parametrize("vazio", ["", None])
    def test_valores_vazios(self, vazio):
        assert secret_encrypt(vazio) == ""
        assert secret_decrypt(vazio) == ""


class TestCompatibilidadeLegado:
    def test_envelope_antigo_continua_a_funcionar(self):
        # O frontend ainda usa este esquema no login; não pode partir.
        assert aes_decrypt(aes_encrypt(SEGREDO)) == SEGREDO

    def test_secret_decrypt_le_formato_antigo(self):
        # Permite migração gradual: linhas por migrar continuam legíveis.
        assert secret_decrypt(aes_encrypt(SEGREDO)) == SEGREDO


class TestMigracao:
    def test_converte_legado_para_v2(self):
        migrado = reencrypt_at_rest(aes_encrypt(SEGREDO))

        assert is_encrypted_at_rest(migrado)
        assert secret_decrypt(migrado) == SEGREDO

    def test_idempotente(self):
        # Correr a migração duas vezes não pode re-cifrar nem corromper.
        uma_vez = reencrypt_at_rest(aes_encrypt(SEGREDO))

        assert reencrypt_at_rest(uma_vez) == uma_vez

    def test_vazio_nao_rebenta(self):
        assert reencrypt_at_rest(None) == ""
        assert reencrypt_at_rest("") == ""


class TestFormatoDeTransporte:
    """
    `to_wire` é o inverso de `reencrypt_at_rest`: converte o valor guardado
    para o envelope que o frontend sabe abrir. Sem isto, as rotas que
    devolvem host/username/password enviariam "v2.…" que o cliente não lê.
    """

    def test_valor_em_repouso_fica_legivel_pelo_frontend(self):
        guardado = secret_encrypt(SEGREDO)

        wire = to_wire(guardado)

        assert not wire.startswith("v2.")
        assert aes_decrypt(wire) == SEGREDO

    def test_funciona_com_linhas_ainda_nao_migradas(self):
        assert aes_decrypt(to_wire(aes_encrypt(SEGREDO))) == SEGREDO

    @pytest.mark.parametrize("vazio", ["", None])
    def test_vazios(self, vazio):
        assert to_wire(vazio) == ""


def test_generate_master_key_produz_chaves_distintas():
    assert generate_master_key() != generate_master_key()
