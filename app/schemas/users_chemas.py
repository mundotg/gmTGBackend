from datetime import datetime
from typing import Any, Optional, Annotated
from pydantic import BaseModel, ConfigDict, EmailStr, Field, StringConstraints, field_validator


# -----------------------------
# 🏢 Empresa Schema
# -----------------------------
class EmpresaSchema(BaseModel):
    id: Optional[int] = None
    nome: str = Field(..., alias="company")
    tamanho: Optional[str] = Field(None, alias="companySize")
    nif: Optional[str] = None
    endereco: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -----------------------------
# 🧩 Cargo Schema
# -----------------------------
class CargoSchema(BaseModel):
    id: Optional[int] = None
    nome: str = Field(..., alias="position")
    descricao: Optional[str] = None
    nivel: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -----------------------------
# 👤 User Create Schema
# -----------------------------
class UserCreate(BaseModel):
    nome: str = Field(..., alias="firstName")
    apelido: str = Field(..., alias="lastName")
    email: EmailStr
    telefone: str = Field(..., alias="phone")
    # userName: str =Field(...,)
    # telefone2 =Column(String(30), nullable=True)

    # Substitui campos diretos por objetos relacionados
    empresa: EmpresaSchema = Field(..., alias="companyData")
    cargo: Optional[CargoSchema] = Field(None, alias="positionData")

    senha: Annotated[str, StringConstraints(min_length=8)] = Field(..., alias="password")
    confirmar_senha: Annotated[str, StringConstraints(min_length=8)] = Field(..., alias="confirmPassword")
    concorda_termos: bool = Field(..., alias="terms")

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "firstName": "João",
                "lastName": "Silva",
                "email": "joao@email.com",
                "phone": "+244900000000",
                "companyData": {
                    "company": "Empresa Lda",
                    "companySize": "11-50"
                },
                "positionData": {
                    "position": "CEO",
                    "descricao": "Diretor Executivo"
                },
                "password": "SenhaForte@123",
                "confirmPassword": "SenhaForte@123",
                "terms": True
            }
        }
    )

    @field_validator("confirmar_senha")
    @classmethod
    def passwords_match(cls, v, info):
        senha = info.data.get("senha")
        if senha and v != senha:
            raise ValueError("As senhas não coincidem.")
        return v


# -----------------------------
# 💾 DB Info Schema
# -----------------------------
class Db_on(BaseModel):
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


# -----------------------------
# 👤 User Output Schema
# -----------------------------
class UserOut(BaseModel):
    id: int
    email: EmailStr
    # userName: str =Field(...,)
    # telefone2 =Column(String(30), nullable=True)
    nome: str
    apelido: str
    telefone: Optional[str] = None
    empresa: Optional[EmpresaSchema] = None
    cargo: Optional[CargoSchema] = None
    InfPlus: Optional[Db_on] = None

    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# 🔐 Login e Token Schemas
# -----------------------------
class LoginResponse(BaseModel):
    user: UserOut


class UserLogin(BaseModel):
    email: EmailStr
    senha: str


class TokenOut(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str


class AccessTokenOut(BaseModel):
    access_token: str
    token_type: str


# -----------------------------
# 📄 Paginação Genérica
# -----------------------------
class PaginacaoOutput(BaseModel):
    page: int
    limit: int
    total: int
    results: list[Any]

    model_config = ConfigDict(from_attributes=True)
