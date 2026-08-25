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


class EmpresaUpdateSchema(BaseModel):
    """Campos editáveis da empresa (todos opcionais → atualização parcial)."""

    nome: Optional[Annotated[str, StringConstraints(strip_whitespace=True, min_length=2, max_length=150)]] = Field(
        None, alias="company"
    )
    tamanho: Optional[Annotated[str, StringConstraints(strip_whitespace=True, max_length=50)]] = Field(
        None, alias="companySize"
    )
    nif: Optional[Annotated[str, StringConstraints(strip_whitespace=True, max_length=50)]] = None
    endereco: Optional[Annotated[str, StringConstraints(strip_whitespace=True, max_length=255)]] = None

    model_config = ConfigDict(populate_by_name=True)


class EmpresaComPermissoesSchema(EmpresaSchema):
    """Empresa + o que este utilizador pode fazer sobre ela (para a UI)."""

    can_manage: bool = False


# -----------------------------
# 🛡️ Permission Schema
# -----------------------------
class PermissionSchema(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    # Derivada do prefixo do nome ("db_connection:read" → "Conexões de BD").
    # Serve só para a UI agrupar as permissões.
    category: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# 🔑 Role Schema
# -----------------------------
class RoleSchema(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    permissions: list[PermissionSchema] = []

    # Roles criadas pelo seed: não podem ser apagadas nem renomeadas.
    is_system: bool = False
    # Role de super admin: nem as permissões podem ser alteradas.
    is_locked: bool = False
    is_active: bool = True
    users_count: int = 0

    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# 🔑 Role — escrita (RBAC admin)
# -----------------------------
class RoleCreateSchema(BaseModel):
    name: str = Field(..., min_length=2, max_length=50)
    description: Optional[str] = Field(None, max_length=200)
    permission_ids: List[int] = []

    @field_validator("name")
    @classmethod
    def normalize_role_name(cls, v: str) -> str:
        normalized = " ".join((v or "").split()).lower()
        if not normalized:
            raise ValueError("O nome da função é obrigatório.")
        return normalized


class RoleUpdateSchema(BaseModel):
    name: Optional[str] = Field(None, min_length=2, max_length=50)
    description: Optional[str] = Field(None, max_length=200)
    is_active: Optional[bool] = None

    @field_validator("name")
    @classmethod
    def normalize_role_name(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        normalized = " ".join(v.split()).lower()
        if not normalized:
            raise ValueError("O nome da função é obrigatório.")
        return normalized


class RolePermissionsUpdateSchema(BaseModel):
    """Substitui integralmente o conjunto de permissões da role."""

    permission_ids: List[int]


# -----------------------------
# 👥 Membros da equipa (RBAC)
# -----------------------------
class MemberSchema(BaseModel):
    id: int
    nome: str
    apelido: Optional[str] = None
    email: str
    is_active: bool = True

    role_id: Optional[int] = None
    role_name: Optional[str] = None
    is_superadmin: bool = False

    model_config = ConfigDict(from_attributes=True)


class MemberRoleUpdateSchema(BaseModel):
    """`role_id = None` retira a função ao membro (fica sem permissões)."""

    role_id: Optional[int] = None


class MemberStatusUpdateSchema(BaseModel):
    is_active: bool


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
