# app/nexus_seed.py
from datetime import datetime
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models.nexus_models import NexusPost, NexusComment
from app.models.user_model import User

def seed_nexus(db: Session):
    existing = db.query(NexusPost).count()
    if existing > 0:
        print(f"Nexus já possui {existing} publicações. Seeding ignorado.")
        return

    admin_user = db.query(User).filter(User.id == 1).first() or db.query(User).first()
    if not admin_user:
        print("Nenhum usuário encontrado para associar artigos.")
        return

    art1 = NexusPost(
        author_id=admin_user.id,
        title="Consolidação Sináptica no Sono REM e a Retenção de Conhecimento Complexo",
        summary="Como as oscilações teta e os fusos de sono reativam redes neurais hipocampais para transferir memórias de curto prazo para o córtex pré-frontal.",
        body_markdown="""## Introdução e Fundamentação

A consolidação de memória não é um processo passivo de decaimento temporal, mas sim uma reorganização biológica ativa que ocorre preferencialmente durante os estágios de sono profundo e sono de movimentos oculares rápidos (REM).

### Mecanismos Moleculares
Durante o sono de ondas lentas (NREM), padrões de disparo síncronos no hipocampo conhecidos como *sharp-wave ripples* (SWRs) reproduzem sequências neuronais vivenciadas durante a vigília. Este fenômeno, denominado **reativação hipocampal**, sinaliza ao neocórtex as conexões sinápticas que devem sofrer potenciação a longo prazo (LTP).

```python
# Simulação simplificada de pesos sinápticos antes e depois do sono
def simulate_synaptic_weight(initial_weight, replay_events, decay_factor=0.98):
    weight = initial_weight
    for _ in range(replay_events):
        weight = (weight * decay_factor) + 0.15
    return min(weight, 1.0)

print(f"Potenciação consolidada: {simulate_synaptic_weight(0.2, 50):.2f}")
```

### Evidência Empírica
Estudos longitudinais com fMRI funcional demonstraram que indivíduos privados de sono após sessões de aprendizagem de conceitos abstratos sofrem uma perda de retenção de até 40% em comparação com grupos com períodos preservados de sono.

| Fase do Sono | Tipo de Memória Consolidada | Biomarcadores Principais |
| :--- | :--- | :--- |
| NREM Estágio 3 | Memória Declarativa e Fatos | Ondas Delta (< 4 Hz), Fusos |
| Sono REM | Memória Procedimental e Criativa | Ritmo Teta (4-8 Hz), Acetilcolina |

### Conclusão
O sono é parte indissociável da engenharia de aprendizagem. Reduzir as horas de descanso reduz a integridade da plasticidade sináptica cortical.""",
        category="Ciência e tecnologia",
        subcategory="Neurociência",
        content_type="cientifico",
        min_age="all",
        status="published",
        read_time_minutes=6,
        cover_image="https://images.unsplash.com/photo-1507413245164-6160d8298b31?w=800&auto=format&fit=crop&q=80",
        tags=["Neurociência", "Aprendizagem", "Sono", "Memória", "Biologia"],
        sources=[
            {
                "title": "Hippocampal replay and the consolidation of declarative memory in human neocortex",
                "authors": "Diekelmann, S. & Born, J.",
                "year": 2024,
                "journal": "Nature Reviews Neuroscience",
                "doi": "10.1038/nrn.2024.182",
                "verified": True,
            }
        ],
        mentions=[{"name": "Prof. António Damásio", "role": "Neurocientista"}],
        is_verified=True,
        views_count=1420,
        likes_count=89,
        shares_count=23,
        bookmarks_count=64,
        comments_count=2,
        published_at=datetime.utcnow(),
    )

    art2 = NexusPost(
        author_id=admin_user.id,
        title="Efeito Mpemba e a Termodinâmica Anómala da Água Sob Convecção Rápida",
        summary="A investigação experimental em torno do contra-intuitivo fenómeno no qual água morna congela mais rapidamente que água fria.",
        body_markdown="""## O Fenómeno e o Paradoxo

O Efeito Mpemba descreve a observação documentada onde amostras de água com temperaturas iniciais superiores atingem o ponto de congelamento antes de amostras análogas em temperaturas inferiores sob as mesmas condições ambientais.

### Hipóteses Mecanísticas sob Escrutínio
1. **Gradientes de Convecção Térmica:** O fluido aquecido induz correntes convectivas mais intensas, acelerando a perda de calor pela interface ar-líquido.
2. **Evaporação de Massa:** A redução de volume causada pela evaporação diminui a capacidade térmica do sistema.
3. **Desgaseificação de Gases Dissolvidos:** O aquecimento expulsa dióxido de carbono e oxigénio dissolvidos, alterando a cinética de nucleação de cristais de gelo.

### Protocolos de Reprodutibilidade
Experiências laboratoriais rigorosas demonstraram que o efeito depende sensivelmente da geometria do recipiente e da condutividade térmica do substrato de apoio.""",
        category="Ciência e tecnologia",
        subcategory="Termodinâmica",
        content_type="curiosidade",
        min_age="all",
        status="published",
        read_time_minutes=4,
        cover_image="https://images.unsplash.com/photo-1518709268805-4e9042af9f23?w=800&auto=format&fit=crop&q=80",
        tags=["Física", "Química", "Termodinâmica", "Curiosidade"],
        sources=[
            {
                "title": "A review of the Mpemba effect: Mechanisms and reproducible protocols",
                "authors": "Burridge, H. C. & Linden, P. F.",
                "year": 2023,
                "journal": "Scientific Reports",
                "doi": "10.1038/s41598-023-45678-x",
                "verified": True,
            }
        ],
        is_verified=True,
        views_count=980,
        likes_count=54,
        shares_count=18,
        bookmarks_count=37,
        comments_count=0,
        published_at=datetime.utcnow(),
    )

    art3 = NexusPost(
        author_id=admin_user.id,
        title="Navegação Espacial e a Degeneração do Córtex Entorrinal no Envelhecimento",
        summary="Como os neurónios em grelha (grid cells) perdem a precisão de disparo e como o treino cognitivo espacial pode desacelerar o declínio neurodegenerativo.",
        body_markdown="""## A Bússola do Cérebro

O córtex entorrinal medial (MEC) abriga os neurónios em grelha (*grid cells*), descobertos pelos laureados Edvard e May-Britt Moser. Estas células disparam em padrões hexagonais periódicos que criam uma métrica interna para a navegação no espaço.

### Vulnerabilidade Pré-Clínica
O córtex entorrinal é uma das primeiras regiões a exibir hiperfosforilação da proteína Tau e deposição de placas beta-amilóides em fases assintomáticas de patologias neurodegenerativas.

### Estratégias de Intervenção
Estudos recentes indicam que tarefas de navegação ativa em ambientes de realidade virtual tridimensional estimulam a perfusão vascular no lobo temporal medial.""",
        category="Saúde e biologia",
        subcategory="Neurobiologia",
        content_type="analise",
        min_age="16+",
        status="published",
        read_time_minutes=7,
        cover_image="https://images.unsplash.com/photo-1559757175-5700dde675bc?w=800&auto=format&fit=crop&q=80",
        tags=["Medicina", "Neurologia", "Envelhecimento", "Cérebro"],
        sources=[
            {
                "title": "Grid cell disruption in pre-symptomatic Alzheimer's disease",
                "authors": "Kunz, L. et al.",
                "year": 2024,
                "journal": "Science",
                "doi": "10.1126/science.aac8128",
                "verified": True,
            }
        ],
        is_verified=True,
        views_count=2150,
        likes_count=132,
        shares_count=45,
        bookmarks_count=98,
        comments_count=0,
        published_at=datetime.utcnow(),
    )

    db.add_all([art1, art2, art3])
    db.commit()
    db.refresh(art1)

    # Adiciona comentários iniciais no art1
    c1 = NexusComment(
        post_id=art1.id,
        user_id=admin_user.id,
        content="Excelente síntese sobre a consolidação sináptica. A correlação com o sono de ondas lentas é essencial para protocolos de estudo intensivo.",
        likes_count=12,
    )
    c2 = NexusComment(
        post_id=art1.id,
        user_id=admin_user.id,
        content="A clareza metodológica deste artigo facilita muito aplicar os conceitos na rotina de aprendizagem contínua.",
        likes_count=5,
    )
    db.add_all([c1, c2])
    db.commit()
    print("Seeding do Nexus Cognitiva concluído com sucesso!")

if __name__ == "__main__":
    db = SessionLocal()
    try:
        seed_nexus(db)
    finally:
        db.close()
