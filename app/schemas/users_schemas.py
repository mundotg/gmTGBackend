from datetime import datetime
from typing import Any, Optional, List, Union
from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from typing_extensions import Annotated


# =============================
# 🏢 Empresa Schema
# =============================
class EmpresaSchema(BaseModel):
    id: Optional[int] = None
    nome: str = Field(..., alias="company")
    tamanho: Optional[str] = Field(None, alias="companySize")
    nif: Optional[str] = None
    endereco: Optional[str] = None

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
    )


# -----------------------------
# 🛡️ Permission Schema
# -----------------------------
class PermissionSchema(BaseModel):
    id: int
    name: str
    description: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# 🔑 Role Schema
# -----------------------------
class RoleSchema(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    permissions: list[PermissionSchema] = []

    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# 🔑 Role Simple (UserOut)
# -----------------------------
class RoleSimpleSchema(BaseModel):
    name: str

    model_config = ConfigDict(from_attributes=True)


# =============================
# 🧩 Cargo Schema
# =============================
class CargoSchema(BaseModel):
    id: Optional[int] = None
    nome: str = Field(..., alias="position")
    descricao: Optional[str] = None
    nivel: Optional[str] = None

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
    )


# =============================
# 👤 User Create Schema
# =============================
class UserCreate(BaseModel):
    nome: str = Field(..., alias="firstName", min_length=2)
    apelido: str = Field(..., alias="lastName", min_length=2)
    email: EmailStr
    telefone: str = Field(..., alias="phone")

    empresa: EmpresaSchema = Field(..., alias="companyData")
    cargo: Optional[CargoSchema] = Field(None, alias="positionData")

    senha: Annotated[str, StringConstraints(min_length=8)] = Field(
        ..., alias="password"
    )
    confirmar_senha: Annotated[str, StringConstraints(min_length=8)] = Field(
        ..., alias="confirmPassword"
    )

    concorda_termos: bool = Field(..., alias="terms")

    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,  # 🔥 remove espaços automaticamente
        json_schema_extra={
            "example": {
                "firstName": "João",
                "lastName": "Silva",
                "email": "joao@email.com",
                "phone": "+244900000000",
                "companyData": {
                    "company": "Empresa Lda",
                    "companySize": "11-50",
                },
                "positionData": {
                    "position": "CEO",
                    "descricao": "Diretor Executivo",
                },
                "password": "SenhaForte@123",
                "confirmPassword": "SenhaForte@123",
                "terms": True,
            }
        },
    )

    # -------------------------
    # 📧 Email
    # -------------------------
    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str):
        return v.lower()

    # -------------------------
    # 👤 Nome
    # -------------------------
    @field_validator("nome", "apelido")
    @classmethod
    def normalize_name(cls, v: str):
        return " ".join(v.title().split())

    # -------------------------
    # 🔐 Senhas (melhor abordagem)
    # -------------------------
    @model_validator(mode="after")
    def validate_passwords(self):
        if self.senha != self.confirmar_senha:
            raise ValueError("Passwords do not match.")
        return self

    # -------------------------
    # 📜 Termos obrigatórios
    # -------------------------
    @field_validator("concorda_termos")
    @classmethod
    def validate_terms(cls, v: bool):
        if not v:
            raise ValueError("Aceitação dos termos é obrigatória.")
        return v


# =============================
# 💾 DB Info Schema
# =============================
class DbInfoSchema(BaseModel):
    id_connection: int
    name_db: str
    data: datetime
    type: str
    num_table: int
    num_consultas: int
    ultima_execucao_ms: Optional[int] = None
    ultima_consulta_em: Optional[datetime] = None
    registros_analizados: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


# =============================
# 👤 User Output Schema (FINAL)
# =============================
class UserOut(BaseModel):
    id: int
    nome: str
    apelido: str
    email: EmailStr
    telefone: Optional[str] = None

    empresa: Optional[EmpresaSchema] = None
    cargo: Optional[CargoSchema] = None

    roles: Optional[RoleSimpleSchema] = None
    permissions: list[str] = []

    info_extra: Optional[DbInfoSchema] = None

    model_config = ConfigDict(from_attributes=True)


class UserOut2(BaseModel):
    id: str
    nome: str
    apelido: str
    email: str
    telefone: Optional[str] = None

    empresa: Optional[EmpresaSchema] = None
    cargo: Optional[CargoSchema] = None

    roles: Optional[List[RoleSimpleSchema]] = None
    permissions: list[str] = []

    info_extra: Optional[DbInfoSchema] = None

    model_config = ConfigDict(from_attributes=True)


# =============================
# 🔐 Auth Schemas
# =============================
class UserLogin(BaseModel):
    email: EmailStr
    senha: str


class TokenOut(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class LoginResponse(BaseModel):
    # O utilizador pode ser UserOut, UserOut2, ou None
    user: Optional[Union[UserOut, UserOut2]] = None
    tokens: Optional[TokenOut] = None


class AccessTokenOut(BaseModel):
    access_token: str
    token_type: Optional[str] = "cookie"


# =============================
# 📄 Paginação Genérica
# =============================
class PaginationOutput(BaseModel):
    page: int
    limit: int
    total: int
    results: List[Any]

    model_config = ConfigDict(from_attributes=True)
