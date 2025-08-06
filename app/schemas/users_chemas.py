from datetime import datetime
from typing import Any, Optional, Annotated
from pydantic import BaseModel, ConfigDict, EmailStr, Field, StringConstraints, field_validator

class UserCreate(BaseModel):
    nome: str = Field(..., alias="firstName")
    apelido: str = Field(..., alias="lastName")
    email: EmailStr
    telefone: str = Field(..., alias="phone")
    nome_empresa: str = Field(..., alias="company")
    cargo: Optional[str] = Field(None, alias="position")
    tamanho_empresa: Optional[str] = Field(None, alias="companySize")

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
                "company": "Empresa Lda",
                "position": "CEO",
                "companySize": "11-50",
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
    
    

class UserOut(BaseModel):
    id: int
    email: EmailStr
    nome: str
    apelido: str
    InfPlus: Optional[Db_on]=None

    model_config = ConfigDict(from_attributes=True)


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
    
class PagitacionOutput(BaseModel):
    page: int
    limit: int
    total: int
    results: list[Any]  # Ou list[UserOut], list[QueryHistory], etc

    model_config = ConfigDict(from_attributes=True)

