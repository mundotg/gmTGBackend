import os
import pickle
import time
from typing import Dict
from app.config.dotenv import get_env_bool, get_env, get_env_int

# -------------------------------
# Configurações do .env
# -------------------------------

CACHE_BINARIO_ENABLED = get_env_bool('CACHE_BINARIO_ENABLED', True)
CACHE_BINARIO_FILE = get_env('CACHE_BINARIO_FILE', 'offset_cache.bin')
CACHE_BINARIO_AUTO_SAVE = get_env_bool('CACHE_BINARIO_AUTO_SAVE', True)
CACHE_BINARIO_SAVE_INTERVAL = get_env_int('CACHE_BINARIO_SAVE_INTERVAL', 60)
CACHE_BINARIO_COMPRESSION = get_env_bool('CACHE_BINARIO_COMPRESSION', False)

# Variáveis de controle
_ultimo_salvamento = time.time()
_cache_modificado = False

# -------------------------------
# Funções utilitárias de cache
# -------------------------------

def salvar_cache_binario(cache: Dict[str, Dict[str, int]]):
    """Grava o cache de offsets no ficheiro binário."""
    if not CACHE_BINARIO_ENABLED:
        return
        
    try:
        protocol = pickle.HIGHEST_PROTOCOL
        with open(CACHE_BINARIO_FILE, "wb") as f:
            pickle.dump({
                'cache_data': cache,
                'metadata': {
                    'timestamp': time.time(),
                    'version': '1.0',
                    'total_tables': len(cache),
                    'total_offsets': sum(len(offsets) for offsets in cache.values())
                }
            }, f, protocol=protocol)
        
        global _ultimo_salvamento, _cache_modificado
        _ultimo_salvamento = time.time()
        _cache_modificado = False
        
        print(f"Cache binário salvo: {len(cache)} tabelas, {sum(len(offsets) for offsets in cache.values())} offsets")
        
    except Exception as e:
        print(f"Erro ao salvar cache binário: {e}")

def carregar_cache_binario() -> Dict[str, Dict[str, int]]:
    """Carrega o cache binário se existir."""
    if not CACHE_BINARIO_ENABLED:
        return {}
        
    if not os.path.exists(CACHE_BINARIO_FILE):
        print("Cache binário não encontrado, iniciando novo cache")
        return {}
    
    try:
        with open(CACHE_BINARIO_FILE, "rb") as f:
            data = pickle.load(f)
            
        # Verifica se é o formato novo com metadados
        if isinstance(data, dict) and 'cache_data' in data:
            cache = data['cache_data']
            metadata = data.get('metadata', {})
            print(f"Cache binário carregado: {metadata.get('total_tables', 0)} tabelas, {metadata.get('total_offsets', 0)} offsets")
        else:
            # Formato antigo (backward compatibility)
            cache = data
            print(f"Cache binário carregado (formato antigo): {len(cache)} tabelas")
            
        return cache
        
    except Exception as e:
        print(f"Erro ao carregar cache binário: {e}")
        return {}

def salvar_cache_se_necessario(cache: Dict[str, Dict[str, int]]):
    """Salva o cache apenas se necessário (auto-save)."""
    global _ultimo_salvamento, _cache_modificado
    
    if not CACHE_BINARIO_ENABLED or not CACHE_BINARIO_AUTO_SAVE:
        return
        
    tempo_atual = time.time()
    tempo_decorrido = tempo_atual - _ultimo_salvamento
    
    if _cache_modificado and tempo_decorrido >= CACHE_BINARIO_SAVE_INTERVAL:
        salvar_cache_binario(cache)

def marcar_cache_como_modificado():
    """Marca o cache como modificado para salvar posteriormente."""
    global _cache_modificado
    _cache_modificado = True

def limpar_cache_binario():
    """Remove o arquivo de cache binário."""
    if os.path.exists(CACHE_BINARIO_FILE):
        try:
            os.remove(CACHE_BINARIO_FILE)
            global obter_chaves_estrangeira_offset_cache
            obter_chaves_estrangeira_offset_cache = {}
            print("Cache binário limpo")
            return True
        except Exception as e:
            print(f"Erro ao limpar cache binário: {e}")
            return False
    return True

def get_cache_binario_info() -> Dict:
    """Retorna informações sobre o cache binário."""
    info = {
        'enabled': CACHE_BINARIO_ENABLED,
        'file_path': CACHE_BINARIO_FILE,
        'file_exists': os.path.exists(CACHE_BINARIO_FILE),
        'auto_save': CACHE_BINARIO_AUTO_SAVE,
        'save_interval': CACHE_BINARIO_SAVE_INTERVAL,
        'cache_size': len(obter_chaves_estrangeira_offset_cache),
        'total_tables': len(obter_chaves_estrangeira_offset_cache),
        'total_offsets': sum(len(offsets) for offsets in obter_chaves_estrangeira_offset_cache.values()),
        'last_modified': _ultimo_salvamento if '_ultimo_salvamento' in globals() else 0,
        'modified': _cache_modificado if '_cache_modificado' in globals() else False
    }
    
    if info['file_exists']:
        info['file_size'] = os.path.getsize(CACHE_BINARIO_FILE)
        info['file_size_mb'] = round(info['file_size'] / (1024 * 1024), 2)
        info['file_mtime'] = os.path.getmtime(CACHE_BINARIO_FILE)
    else:
        info['file_size'] = 0
        info['file_size_mb'] = 0
        info['file_mtime'] = 0
        
    return info

def backup_cache_binario(backup_suffix: str = None):
    """Cria um backup do cache binário."""
    if not os.path.exists(CACHE_BINARIO_FILE):
        print("Nenhum cache para fazer backup")
        return False
        
    if backup_suffix is None:
        backup_suffix = time.strftime("%Y%m%d_%H%M%S")
        
    backup_file = f"{CACHE_BINARIO_FILE}.backup.{backup_suffix}"
    
    try:
        import shutil
        shutil.copy2(CACHE_BINARIO_FILE, backup_file)
        print(f"Backup do cache criado: {backup_file}")
        return True
    except Exception as e:
        print(f"Erro ao criar backup do cache: {e}")
        return False

# Cache global carregado do disco
obter_chaves_estrangeira_offset_cache = carregar_cache_binario()

# Exemplo de uso das funções
if __name__ == "__main__":
    # Exemplo de uso
    print("=== Cache Binário de Offsets ===")
    info = get_cache_binario_info()
    for key, value in info.items():
        print(f"{key}: {value}")
    
    # Exemplo de modificação do cache
    if obter_chaves_estrangeira_offset_cache:
        tabela_exemplo = list(obter_chaves_estrangeira_offset_cache.keys())[0]
        print(f"\nExemplo de offsets para tabela '{tabela_exemplo}':")
        print(obter_chaves_estrangeira_offset_cache[tabela_exemplo])
    
    # Backup automático
    backup_cache_binario()
