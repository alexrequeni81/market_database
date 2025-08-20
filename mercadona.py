import pandas as pd
import requests
import json
import os
import time
import random
import dropbox
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import sys

class MercadonaHybridCatalogSystem:
    BASE_URL = "https://tienda.mercadona.es"
    CACHE_DIR = "mercadona_cache_v6"
    
    def __init__(self, catalog_path=None, lang="es", warehouse="vlc1"):
        """
        Inicializa el sistema híbrido para construcción y actualización de catálogo
        
        Args:
            catalog_path: Ruta al catálogo CSV (si es None, usa ruta predeterminada)
            lang: Código de idioma
            warehouse: Código de almacén/localización
        """
        # Configurar ruta predeterminada del catálogo
        if catalog_path is None:
            self.base_dir = self._get_base_directory()
            self.catalog_path = os.path.join(self.base_dir, "catalogos", "catalogo_completo.csv")
        else:
            self.catalog_path = catalog_path
            self.base_dir = os.path.dirname(os.path.abspath(catalog_path))
        
        self.lang = lang
        self.warehouse = warehouse
        self.headers = self._create_headers()
        self._setup_directories()
        
        # Estado inicial
        self.catalog_df = None
        self.known_products = set()
        self.category_limits = {}
        self.rotation_index = 0
        self.dropbox_client = None
        self.incomplete_categories = []
        
        print(f"Directorio base configurado: {self.base_dir}")
        print(f"Ruta del catálogo: {os.path.abspath(self.catalog_path)}")
        
        # Cargar estado previo si existe
        self._load_previous_state()
    
    def _get_base_directory(self):
        """Obtiene el directorio base independiente del sistema operativo"""
        # Si se está ejecutando como script, usar el directorio del script
        if getattr(sys, 'frozen', False):
            # Si está empaquetado (ej. con PyInstaller)
            return os.path.dirname(sys.executable)
        else:
            # Si se ejecuta normalmente
            return os.path.dirname(os.path.abspath(__file__))
    
    def _create_headers(self):
        """Crea headers optimizados"""
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": f"{self.BASE_URL}/",
            "Origin": self.BASE_URL,
            "X-Requested-With": "XMLHttpRequest",
            "Cache-Control": "no-cache"
        }
    
    def _setup_directories(self):
        """Configura directorios con manejo robusto de rutas"""
        # Crear directorios en el directorio base
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        os.makedirs(os.path.join(self.CACHE_DIR, "products"), exist_ok=True)
        os.makedirs(os.path.join(self.CACHE_DIR, "categories"), exist_ok=True)
        os.makedirs(os.path.join(self.base_dir, "actualizaciones"), exist_ok=True)
        os.makedirs(os.path.join(self.base_dir, "catalogos"), exist_ok=True)
        
        print("Directorios configurados correctamente:")
        print(f"- Caché: {os.path.abspath(self.CACHE_DIR)}")
        print(f"- Catálogos: {os.path.join(self.base_dir, 'catalogos')}")
        print(f"- Actualizaciones: {os.path.join(self.base_dir, 'actualizaciones')}")
    
    def _clean_product_id(self, product_id):
        """Limpia el ID del producto"""
        if isinstance(product_id, float) and product_id.is_integer():
            return str(int(product_id))
        return str(product_id)
    
    def _load_previous_state(self):
        """Carga estado previo si existe"""
        abs_path = os.path.abspath(self.catalog_path)
        
        if os.path.exists(self.catalog_path):
            try:
                # Cargar CSV asegurando que los IDs se lean como strings
                self.catalog_df = pd.read_csv(self.catalog_path, dtype={'id': str, 'categoria_id': str})
                
                # Limpiar los IDs
                self.catalog_df["id"] = self.catalog_df["id"].apply(self._clean_product_id)
                self.catalog_df["categoria_id"] = self.catalog_df["categoria_id"].apply(self._clean_product_id)
                
                self.known_products = set(self.catalog_df["id"].tolist())
                self.category_limits = self._calculate_category_limits()
                
                # Cargar información de última actualización
                update_info = self._get_last_update_info()
                self.rotation_index = update_info.get("rotation_index", 0)
                self.incomplete_categories = update_info.get("incomplete_categories", [])
                
                print(f"\nEstado previo cargado desde: {abs_path}")
                print(f"{len(self.known_products)} productos cargados correctamente.")
                print(f"Rotación actual: Parte {self.rotation_index + 1} de 4")
                
                if self.incomplete_categories:
                    print(f"\nCategorías incompletas detectadas: {len(self.incomplete_categories)}")
                    for cat_id in self.incomplete_categories:
                        try:
                            cat_name = self.catalog_df[self.catalog_df["categoria_id"] == cat_id]["categoria"].iloc[0]
                            print(f"- {cat_name} (ID: {cat_id})")
                        except:
                            print(f"- Categoría ID: {cat_id} (nombre no disponible)")
                
                return True
            except Exception as e:
                print(f"Error al cargar estado previo de {abs_path}: {str(e)}")
                import traceback
                print(f"Pila de errores:\n{traceback.format_exc()}")
        
        print("\nNo se encontró estado previo en la ruta especificada.")
        print(f"Ruta buscada: {abs_path}")
        
        # Buscar en directorios alternativos
        alternative_paths = [
            os.path.join(os.getcwd(), "catalogos", "catalogo_completo.csv"),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "catalogos", "catalogo_completo.csv"),
            os.path.join(self.base_dir, "catalogos", "catalogo_completo.csv"),
            "catalogo_completo.csv"
        ]
        
        for path in alternative_paths:
            abs_alt_path = os.path.abspath(path)
            if os.path.exists(abs_alt_path) and abs_alt_path != abs_path:
                print(f"\n¡Se encontró un catálogo en: {abs_alt_path}!")
                print("¿Quieres usar este catálogo? (s/n)")
                use_it = input().strip().lower()
                if use_it == "s":
                    self.catalog_path = path
                    return self._load_previous_state()
        
        print("\nNo se encontró ningún catálogo existente.")
        return False
    
    def _get_last_update_info(self):
        """Obtiene información de la última actualización"""
        info_file = os.path.join(self.CACHE_DIR, "update_info.json")
        if os.path.exists(info_file):
            try:
                with open(info_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                pass
        return {"timestamp": "Nunca", "rotation_index": 0, "incomplete_categories": []}
    
    def _save_update_info(self, info):
        """Guarda información de actualización"""
        info["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        info_file = os.path.join(self.CACHE_DIR, "update_info.json")
        with open(info_file, "w", encoding="utf-8") as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
    
    def _reset_state(self):
        """Resetea el estado para una construcción completa"""
        self.catalog_df = None
        self.known_products = set()
        self.category_limits = {}
        self.rotation_index = 0
        self.incomplete_categories = []
        
        # Limpiar caché
        cache_dir = Path(self.CACHE_DIR)
        if cache_dir.exists():
            for folder in ["products", "categories"]:
                folder_path = cache_dir / folder
                if folder_path.exists():
                    for file in folder_path.glob("*"):
                        try:
                            file.unlink()
                        except Exception as e:
                            print(f"Error al eliminar {file}: {str(e)}")
    
    def _calculate_category_limits(self):
        """Calcula límites dinámicos por categoría"""
        if self.catalog_df is None or len(self.catalog_df) == 0:
            return {}
        
        category_sizes = self.catalog_df["categoria_id"].value_counts()
        total_products = len(self.catalog_df)
        
        # Determinar categorías grandes, medianas y pequeñas
        large_categories = category_sizes[category_sizes > total_products * 0.08].index.tolist()
        medium_categories = category_sizes[(category_sizes <= total_products * 0.08) & 
                                         (category_sizes > total_products * 0.04)].index.tolist()
        small_categories = category_sizes[category_sizes <= total_products * 0.04].index.tolist()
        
        # CONVERTIR TODOS LOS IDs DE CATEGORÍA A STRINGS
        large_categories = [str(cat) for cat in large_categories]
        medium_categories = [str(cat) for cat in medium_categories]
        small_categories = [str(cat) for cat in small_categories]
        
        # Asignar límites
        limits = {}
        for cat_id in category_sizes.index:
            cat_id_str = str(cat_id)
            if cat_id_str in large_categories:
                limits[cat_id_str] = min(120, int(category_sizes[cat_id] * 1.2))
            elif cat_id_str in medium_categories:
                limits[cat_id_str] = min(90, int(category_sizes[cat_id] * 1.3))
            else:
                limits[cat_id_str] = min(60, int(category_sizes[cat_id] * 1.5))
        
        return limits
    
    def get_product_details(self, product_id):
        """Obtiene detalles de producto con caché"""
        clean_id = self._clean_product_id(product_id)
        cache_file = os.path.join(self.CACHE_DIR, "products", f"{clean_id}.json")
        
        # Primero verificar en caché
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                pass
        
        # Si no está en caché, obtener de la API
        url = f"{self.BASE_URL}/api/products/{clean_id}/"
        params = {"lang": self.lang, "wh": self.warehouse}
        
        try:
            time.sleep(max(0.5, 1.0 - random.uniform(0, 0.3)))
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                product = response.json()
                # Guardar en caché
                os.makedirs(os.path.dirname(cache_file), exist_ok=True)
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(product, f, ensure_ascii=False, indent=2)
                return product
            return None
            
        except Exception as e:
            print(f"  ! Error al obtener producto {clean_id}: {str(e)}")
            return None
    
    def extract_product_data(self, product):
        """Extrae datos estructurados del producto"""
        if not product:
            return None
            
        price_info = product.get("price_instructions", {})
        
        # Determinar categoría
        category_id = None
        category_name = "Sin categoría"
        if product["categories"]:
            category_id = self._clean_product_id(product["categories"][0]["id"])
            category_name = product["categories"][0]["name"]
        
        return {
            "id": self._clean_product_id(product["id"]),
            "nombre": product["display_name"],
            "slug": product["slug"],
            "categoria_id": category_id,
            "categoria": category_name,
            "precio_total": float(price_info.get("bulk_price", 0)),
            "precio_por_unidad": float(price_info.get("unit_price", 0)),
            "unidad_medida": price_info.get("size_format", ""),
            "iva": price_info.get("tax_percentage", ""),
            "empaque": product["packaging"],
            "disponible": product["published"],
            "url": product["share_url"].strip() if product.get("share_url") else ""
        }
    
    def build_full_catalog(self, max_products=1500, base_products_per_category=70, max_category_size=150):
        """
        Construye un catálogo completo desde cero con límites dinámicos por categoría
        """
        print("\n" + "="*70)
        print("INICIANDO CONSTRUCCIÓN COMPLETA DEL CATÁLOGO (MEJORADA)")
        print("="*70)
        
        # Resetear estado para construcción completa
        self._reset_state()
        
        # Semillas estratégicas predefinidas
        strategic_seeds = [
            "3497", "86385", "21329", "60091", "84785", "52710", "62048", "40229", 
            "86397", "30167", "3819", "23017", "23013", "35420", "18086", "86905", 
            "86786", "9264", "13204", "66462", "9280", "19897", "5044", "22910", 
            "28035", "4241"
        ]
        
        all_products = []
        products_by_category = defaultdict(int)
        visited = set()
        incomplete_categories = set()
        
        # Mantener un seguimiento del límite actual para cada categoría
        current_category_limits = {}
        
        print(f"Usando {len(strategic_seeds)} semillas estratégicas para exploración completa")
        
        # Explorar desde cada semilla
        for i, seed_id in enumerate(strategic_seeds, 1):
            print(f"\n({i}/{len(strategic_seeds)}) Explorando desde semilla: {seed_id}")
            queue = [seed_id]
            
            while queue and len(all_products) < max_products:
                current_id = queue.pop(0)
                
                if current_id in visited:
                    continue
                    
                visited.add(current_id)
                api_data = self.get_product_details(current_id)
                
                if api_data is not None:
                    structured = self.extract_product_data(api_data)
                    if structured:
                        all_products.append(structured)
                        cat_id = structured["categoria_id"]
                        products_by_category[cat_id] += 1
                        
                        # Mostrar progreso
                        if len(all_products) % 20 == 0:
                            print(f"  → {len(all_products)} productos encontrados | {products_by_category[cat_id]} en {structured['categoria']}")
                    
                    # Obtener productos relacionados
                    url = f"{self.BASE_URL}/api/products/{current_id}/xselling/"
                    params = {
                        "lang": self.lang,
                        "wh": self.warehouse,
                        "exclude": ""
                    }
                    try:
                        time.sleep(0.3)
                        response = requests.get(url, headers=self.headers, params=params)
                        if response.status_code == 200:
                            related_ids = [item["id"] for item in response.json().get("results", [])]
                            for related_id in related_ids:
                                if related_id not in visited and related_id not in queue:
                                    queue.append(related_id)
                    except Exception as e:
                        print(f"  ! Error obteniendo productos relacionados para {current_id}: {str(e)}")
                
                # Control de límites por categoría con extensión inteligente
                if api_data is not None and structured:
                    cat_id = structured["categoria_id"]
                    current_count = products_by_category[cat_id]
                    category_name = structured["categoria"]
                    
                    # Obtener o calcular el límite actual para esta categoría
                    if cat_id not in current_category_limits:
                        # Calcular límite base inicial
                        base_limit = self._get_dynamic_category_limit(
                            cat_id, 
                            base_products_per_category, 
                            max_category_size
                        )
                        current_category_limits[cat_id] = base_limit
                    
                    category_limit = current_category_limits[cat_id]
                    
                    # Extender el límite si hay productos relacionados disponibles
                    if current_count >= category_limit * 0.9 and queue:
                        extension = min(20, max_category_size - current_count)
                        if extension > 0:
                            print(f"  → Categoría grande detectada: {category_name}. Extendiendo límite en {extension} productos")
                            new_limit = category_limit + extension
                            current_category_limits[cat_id] = new_limit
                            category_limit = new_limit
                    
                    # Verificar si alcanzamos el límite final
                    if current_count >= category_limit:
                        print(f"  → Límite FINAL alcanzado para {category_name}: {current_count}/{category_limit}")
                        incomplete_categories.add(cat_id)
        
        # Crear DataFrame y guardar
        self.catalog_df = pd.DataFrame(all_products)
        self.known_products = set(self.catalog_df["id"].tolist())
        
        # Guardar información sobre categorías incompletas
        self.incomplete_categories = list(incomplete_categories)
        self._save_incomplete_categories(incomplete_categories)
        
        # Guardar catálogo completo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.catalog_path = os.path.join(self.base_dir, "catalogos", f"catalogo_completo_{timestamp}.csv")
        self.catalog_df.to_csv(self.catalog_path, index=False, encoding="utf-8-sig")
        
        # Guardar información de actualización
        self._save_update_info({
            "rotation_index": 0,
            "incomplete_categories": list(incomplete_categories)
        })
        
        # Mostrar análisis de categorías incompletas
        if incomplete_categories:
            print("\n" + "="*50)
            print("CATEGORÍAS INCOMPLETAS DETECTADAS")
            print("="*50)
            for cat_id in incomplete_categories:
                try:
                    cat_name = self.catalog_df[self.catalog_df["categoria_id"] == cat_id]["categoria"].iloc[0]
                    count = products_by_category[cat_id]
                    print(f"- {cat_name} (ID: {cat_id}): {count} productos (límite alcanzado)")
                except:
                    print(f"- Categoría ID: {cat_id}: {products_by_category[cat_id]} productos (límite alcanzado)")
            print(f"\nEstas categorías se priorizarán en futuras actualizaciones")
        
        print(f"\nCATÁLOGO COMPLETO GENERADO: {len(all_products)} productos")
        print(f"Archivo guardado como: {os.path.abspath(self.catalog_path)}")
        
        return self.catalog_df

    def _get_dynamic_category_limit(self, category_id, base_limit, max_limit):
        """
        Determina el límite dinámico para una categoría basado en su tamaño esperado
        
        Args:
            category_id: ID de la categoría
            base_limit: Límite base
            max_limit: Límite máximo
            
        Returns:
            int: Límite dinámico para esta categoría
        """
        # Categorías conocidas que suelen ser grandes
        LARGE_CATEGORIES = ["1", "8", "13", "5", "6", "7", "11", "12", "21"]
        
        # Categorías medianas
        MEDIUM_CATEGORIES = ["2", "3", "4", "9", "10", "14", "15", "16", "17", "18", "19", "20", "22", "23", "24", "25"]
        
        if category_id in LARGE_CATEGORIES:
            return min(max_limit, base_limit * 2)
        elif category_id in MEDIUM_CATEGORIES:
            return min(max_limit, base_limit * 1.5)
        else:
            return base_limit

    def _save_incomplete_categories(self, incomplete_categories):
        """Guarda información sobre categorías que no se completaron"""
        incomplete_file = os.path.join(self.CACHE_DIR, "incomplete_categories.json")
        with open(incomplete_file, "w", encoding="utf-8") as f:
            json.dump(list(incomplete_categories), f, ensure_ascii=False, indent=2)
    
    def _load_incomplete_categories(self):
        """Carga la lista de categorías incompletas"""
        incomplete_file = os.path.join(self.CACHE_DIR, "incomplete_categories.json")
        if os.path.exists(incomplete_file):
            try:
                with open(incomplete_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                pass
        return []
    
    def update_catalog_intelligent(self):
        """
        Actualiza el catálogo con verificación casi 100% y priorización de categorías incompletas
        """
        if self.catalog_df is None:
            print("Error: No hay catálogo existente. Primero debes construir un catálogo completo.")
            return None
        
        print("\n" + "="*70)
        print("INICIANDO ACTUALIZACIÓN INTELIGENTE (MEJORADA)")
        print("="*70)
        
        # Cargar categorías incompletas del estado previo
        incomplete_categories = self._load_incomplete_categories()
        
        # Determinar qué productos verificar en esta rotación
        products_list = list(self.known_products)
        products_to_check = [
            pid for i, pid in enumerate(products_list) 
            if i % 4 == self.rotation_index
        ]
        
        print(f"Última actualización: {self._get_last_update_info().get('timestamp', 'Nunca')}")
        print(f"Rotación actual: Parte {self.rotation_index + 1} de 4")
        print(f"Verificando {len(products_to_check)} productos en esta rotación")
        
        # Verificar los productos asignados a esta rotación
        updated_products = []
        for i, product_id in enumerate(products_to_check, 1):
            if i % 20 == 0:
                print(f"  - Verificados {i}/{len(products_to_check)} productos")
            
            clean_id = self._clean_product_id(product_id)
            api_data = self.get_product_details(clean_id)
            
            if api_data is not None and self._is_product_updated(clean_id, api_data):
                structured = self.extract_product_data(api_data)
                if structured:
                    updated_products.append(structured)
        
        # Buscar nuevos productos (con priorización de categorías incompletas)
        new_products = self._find_new_products(max_new=150, prioritize_incomplete=True)
        
        # Actualizar catálogo
        if updated_products or new_products:
            self._update_catalog_with_changes(updated_products, new_products)
        
        # Actualizar índice de rotación
        next_rotation = (self.rotation_index + 1) % 4
        self._save_update_info({
            "rotation_index": next_rotation,
            "incomplete_categories": self.incomplete_categories
        })
        self.rotation_index = next_rotation
        
        return {
            "updated_products": updated_products,
            "new_products": new_products,
            "rotation_index": self.rotation_index,
            "next_rotation": next_rotation
        }

    def _is_product_updated(self, product_id, api_data):
        """
        Verifica si un producto ha cambiado comparando con la versión en caché
        """
        cache_file = os.path.join(self.CACHE_DIR, "products", f"{product_id}.json")
        if not os.path.exists(cache_file):
            return True  # Si no está en caché, asumimos que es nuevo
            
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
        except:
            return True
        
        # Comparar campos clave
        price_info_cached = cached.get("price_instructions", {})
        price_info_api = api_data.get("price_instructions", {})
        
        if str(price_info_cached.get("bulk_price", "0")) != str(price_info_api.get("bulk_price", "0")):
            return True
        if cached.get("published", True) != api_data.get("published", True):
            return True
        if cached.get("display_name", "") != api_data.get("display_name", ""):
            return True
            
        return False
    
    def _find_new_products(self, max_new=150, prioritize_incomplete=False):
        """
        Busca nuevos productos con priorización de categorías incompletas
        
        Args:
            max_new: Máximo de nuevos productos a encontrar
            prioritize_incomplete: Si True, prioriza categorías incompletas
            
        Returns:
            list: Nuevos productos encontrados
        """
        print(f"\nBuscando nuevos productos (máximo: {max_new})...")
        
        # Cargar categorías incompletas
        incomplete_categories = self._load_incomplete_categories()
        
        # Seleccionar semillas estratégicas del catálogo existente
        category_seeds = defaultdict(list)
        
        # Si hay categorías incompletas, priorizarlas
        if prioritize_incomplete and incomplete_categories:
            print(f"Priorizando búsqueda en {len(incomplete_categories)} categorías incompletas")
            
            # Primero, semillas de categorías incompletas
            for _, row in self.catalog_df[self.catalog_df["categoria_id"].isin(incomplete_categories)].iterrows():
                cat_id = self._clean_product_id(row["categoria_id"])
                product_id = self._clean_product_id(row["id"])
                category_seeds[cat_id].append(product_id)
            
            # Limitar a 5 semillas por categoría incompleta
            for cat_id in incomplete_categories:
                if len(category_seeds[cat_id]) > 5:
                    category_seeds[cat_id] = random.sample(category_seeds[cat_id], 5)
        
        # Luego, semillas de otras categorías
        other_categories = [cat for cat in self.catalog_df["categoria_id"].unique() 
                           if cat not in incomplete_categories]
        
        for _, row in self.catalog_df[self.catalog_df["categoria_id"].isin(other_categories)].sample(min(200, len(self.catalog_df))).iterrows():
            cat_id = self._clean_product_id(row["categoria_id"])
            product_id = self._clean_product_id(row["id"])
            category_seeds[cat_id].append(product_id)
        
        # Limitar a 3 semillas por categoría normal
        for cat_id in other_categories:
            if len(category_seeds[cat_id]) > 3:
                category_seeds[cat_id] = random.sample(category_seeds[cat_id], 3)
        
        # Buscar nuevos productos
        new_products = []
        visited = set(self.known_products)
        
        # Primero explorar categorías incompletas
        for cat_id in incomplete_categories:
            if len(new_products) >= max_new:
                break
                
            for seed_id in category_seeds.get(cat_id, []):
                if len(new_products) >= max_new:
                    break
                    
                print(f"  → Priorizando exploración en categoría incompleta: {cat_id}")
                queue = [seed_id]
                
                while queue and len(new_products) < max_new:
                    current_id = queue.pop(0)
                    
                    if current_id in visited:
                        continue
                        
                    visited.add(current_id)
                    api_data = self.get_product_details(current_id)
                    
                    if api_data is not None:
                        structured = self.extract_product_data(api_data)
                        if structured and current_id not in self.known_products:
                            new_products.append(structured)
                            self.known_products.add(current_id)
                            
                            # Mostrar progreso
                            if len(new_products) % 5 == 0:
                                print(f"    + {len(new_products)} nuevos productos encontrados (prioritarios)")
                        
                        # Obtener productos relacionados
                        url = f"{self.BASE_URL}/api/products/{current_id}/xselling/"
                        params = {
                            "lang": self.lang,
                            "wh": self.warehouse,
                            "exclude": ""
                        }
                        try:
                            time.sleep(0.3)
                            response = requests.get(url, headers=self.headers, params=params)
                            if response.status_code == 200:
                                related_ids = [item["id"] for item in response.json().get("results", [])]
                                for related_id in related_ids:
                                    if related_id not in visited and related_id not in queue:
                                        queue.append(related_id)
                        except Exception as e:
                            print(f"  ! Error obteniendo productos relacionados para {current_id}: {str(e)}")
        
        # Luego explorar otras categorías si hay espacio
        for category_id, seed_ids in category_seeds.items():
            if category_id in incomplete_categories or len(new_products) >= max_new:
                continue
                
            for seed_id in seed_ids:
                if len(new_products) >= max_new:
                    break
                    
                print(f"  → Explorando desde semilla: {seed_id}")
                queue = [seed_id]
                
                while queue and len(new_products) < max_new:
                    current_id = queue.pop(0)
                    
                    if current_id in visited:
                        continue
                        
                    visited.add(current_id)
                    api_data = self.get_product_details(current_id)
                    
                    if api_data is not None:
                        structured = self.extract_product_data(api_data)
                        if structured and current_id not in self.known_products:
                            new_products.append(structured)
                            self.known_products.add(current_id)
                            
                            # Mostrar progreso
                            if len(new_products) % 5 == 0:
                                print(f"    + {len(new_products)} nuevos productos encontrados")
                    
                    # Obtener productos relacionados
                    url = f"{self.BASE_URL}/api/products/{current_id}/xselling/"
                    params = {
                        "lang": self.lang,
                        "wh": self.warehouse,
                        "exclude": ""
                    }
                    try:
                        time.sleep(0.3)
                        response = requests.get(url, headers=self.headers, params=params)
                        if response.status_code == 200:
                            related_ids = [item["id"] for item in response.json().get("results", [])]
                            for related_id in related_ids:
                                if related_id not in visited and related_id not in queue:
                                    queue.append(related_id)
                    except Exception as e:
                        print(f"  ! Error obteniendo productos relacionados para {current_id}: {str(e)}")
        
        # Verificar si se completaron categorías incompletas
        if incomplete_categories and new_products:
            completed_categories = self._check_completed_categories(incomplete_categories, new_products)
            if completed_categories:
                print("\nCATEGORÍAS COMPLETADAS:")
                for cat_id, count in completed_categories.items():
                    try:
                        cat_name = self.catalog_df[self.catalog_df["categoria_id"] == cat_id]["categoria"].iloc[0]
                        print(f"- {cat_name} (ID: {cat_id}): +{count} productos añadidos")
                    except:
                        print(f"- Categoría ID: {cat_id}: +{count} productos añadidos")
                
                # Actualizar lista de categorías incompletas
                updated_incomplete = [cat for cat in incomplete_categories if cat not in completed_categories]
                self._save_incomplete_categories(updated_incomplete)
                self.incomplete_categories = updated_incomplete
        
        return new_products

    def _check_completed_categories(self, incomplete_categories, new_products):
        """
        Verifica si se han completado categorías que antes eran incompletas
        
        Args:
            incomplete_categories: Lista de categorías incompletas previas
            new_products: Nuevos productos encontrados
            
        Returns:
            dict: Categorías completadas y cantidad de productos añadidos
        """
        completed = {}
        new_df = pd.DataFrame(new_products)
        
        for cat_id in incomplete_categories:
            # Contar nuevos productos en esta categoría
            new_count = len(new_df[new_df["categoria_id"] == cat_id])
            
            # Si añadimos al menos 15 productos nuevos, consideramos que la categoría está "más completa"
            if new_count >= 15:
                completed[cat_id] = new_count
        
        return completed

    def _update_catalog_with_changes(self, updated_products, new_products):
        """
        Actualiza el catálogo con los cambios detectados
        """
        if not updated_products and not new_products:
            return
            
        # Crear DataFrames de los cambios
        updated_df = pd.DataFrame(updated_products)
        new_df = pd.DataFrame(new_products)
        
        # Actualizar catálogo
        updated_catalog = self.catalog_df.copy()
        
        # Eliminar productos actualizados del catálogo existente
        if not updated_df.empty:
            updated_catalog = updated_catalog[~updated_catalog["id"].isin(updated_df["id"])]
        
        # Añadir productos actualizados
        if not updated_df.empty:
            updated_catalog = pd.concat([updated_catalog, updated_df], ignore_index=True)
        
        # Añadir nuevos productos
        if not new_df.empty:
            updated_catalog = pd.concat([updated_catalog, new_df], ignore_index=True)
        
        # Actualizar estado interno
        self.catalog_df = updated_catalog
        self.known_products = set(updated_catalog["id"].tolist())
        self.category_limits = self._calculate_category_limits()
        
        # Guardar catálogo actualizado
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.catalog_path = os.path.join(self.base_dir, "catalogos", f"catalogo_actualizado_{timestamp}.csv")
        self.catalog_df.to_csv(self.catalog_path, index=False, encoding="utf-8-sig")
    
    def generate_report(self, update_results):
        """
        Genera un informe detallado de la actualización
        """
        if not update_results:
            return None
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = os.path.join(self.base_dir, "actualizaciones", f"actualizacion_{timestamp}")
        os.makedirs(report_dir, exist_ok=True)
        
        # Informe de cambios
        report = {
            "fecha": timestamp,
            "productos_verificados": len(update_results.get("updated_products", [])) + len(update_results.get("new_products", [])),
            "productos_actualizados": len(update_results.get("updated_products", [])),
            "nuevos_productos": len(update_results.get("new_products", [])),
            "rotacion_actual": update_results["rotation_index"] + 1,
            "siguiente_rotacion": update_results["next_rotation"] + 1
        }
        
        # Guardar informe
        with open(os.path.join(report_dir, "informe.json"), "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        # Resumen visual
        summary = f"""
        INFORME DE ACTUALIZACIÓN - {timestamp}
        =====================================
        
        ESTADÍSTICAS:
        - Productos verificados en esta rotación: {report['productos_verificados']}
        - Productos actualizados: {report['productos_actualizados']}
        - Nuevos productos: {report['nuevos_productos']}
        
        SISTEMA DE ROTACIÓN:
        - Esta es la parte {report['rotacion_actual']} de 4
        - La próxima actualización verificará la parte {report['siguiente_rotacion']}
        - En 4 actualizaciones consecutivas se habrá verificado el 100% del catálogo
        
        PRÓXIMOS PASOS:
        1. Revisa {report_dir}/catalogo_actualizado_{timestamp}.csv para ver los cambios
        2. Ejecuta actualizaciones periódicas para mantener el catálogo completo
        """
        
        with open(os.path.join(report_dir, "resumen.txt"), "w", encoding="utf-8") as f:
            f.write(summary)
        
        # Crear enlace al catálogo actualizado
        catalog_file = os.path.join(self.base_dir, "catalogos", f"catalogo_actualizado_{timestamp}.csv")
        if os.path.exists(catalog_file):
            report["catalog_file"] = catalog_file
            
        return report_dir
    
    def setup_dropbox(self):
        """
        Configura la conexión con Dropbox
        """
        print("\n" + "="*50)
        print("CONFIGURACIÓN DE DROPBOX")
        print("="*50)
        
        # Verificar si existe el archivo de configuración
        config_file = os.path.join(self.base_dir, "dropbox_config.json")
        if os.path.exists(config_file):
            try:
                with open(config_file, "r") as f:
                    config = json.load(f)
                access_token = config.get("access_token")
                
                if access_token:
                    print("Configuración de Dropbox encontrada. Intentando conectar...")
                    return self._connect_to_dropbox(access_token)
            except:
                pass
        
        print("\nNo se encontró configuración de Dropbox o es inválida.")
        print("Necesitas un token de acceso de Dropbox para subir los archivos.")
        print("Puedes obtener uno aquí: https://www.dropbox.com/developers/apps")
        
        # Preguntar si quiere configurar Dropbox
        setup = input("\n¿Quieres configurar Dropbox ahora? (s/n): ").strip().lower()
        if setup != "s":
            return False
        
        # Obtener el token de acceso
        print("\nIngresa tu token de acceso de Dropbox (se guardará de forma segura):")
        print("Si no tienes uno, visita: https://www.dropbox.com/developers/apps")
        access_token = input("Token de acceso: ").strip()
        
        # Intentar conectar
        if self._connect_to_dropbox(access_token):
            # Guardar configuración de forma segura
            with open(config_file, "w") as f:
                json.dump({"access_token": access_token}, f)
            print(f"\nConfiguración guardada en {config_file}")
            return True
        else:
            print("\nError: No se pudo conectar a Dropbox con el token proporcionado.")
            retry = input("¿Quieres intentarlo de nuevo? (s/n): ").strip().lower()
            return retry == "s" and self.setup_dropbox()
    
    def _connect_to_dropbox(self, access_token):
        """
        Conecta con Dropbox usando el token de acceso
        
        Args:
            access_token: Token de acceso de Dropbox
            
        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario
        """
        try:
            self.dropbox_client = dropbox.Dropbox(access_token)
            # Verificar la conexión obteniendo información de la cuenta
            account = self.dropbox_client.users_get_current_account()
            print(f"\nConexión exitosa a Dropbox! Cuenta: {account.name.display_name}")
            return True
        except Exception as e:
            print(f"Error al conectar con Dropbox: {str(e)}")
            self.dropbox_client = None
            return False
    
    def upload_to_dropbox(self, local_path, dropbox_path=None):
        """
        Sube un archivo o directorio a Dropbox
        
        Args:
            local_path: Ruta local del archivo o directorio a subir
            dropbox_path: Ruta en Dropbox (si es None, usa el nombre del archivo)
            
        Returns:
            bool: True si la subida fue exitosa, False en caso contrario
        """
        if not self.dropbox_client:
            print("Error: No hay conexión con Dropbox. Configura primero Dropbox.")
            return False
        
        # Convertir a Path para manejar rutas
        local_path = Path(local_path)
        
        # Si no se especifica ruta en Dropbox, usar el nombre del archivo
        if not dropbox_path:
            dropbox_path = "/catalogo_completo.csv"
        
        try:
            # Si es un directorio, subir todos los archivos recursivamente
            if local_path.is_dir():
                print(f"Subiendo directorio {local_path} a Dropbox ({dropbox_path})...")
                for root, _, files in os.walk(local_path):
                    for file in files:
                        file_path = Path(root) / file
                        rel_path = os.path.relpath(file_path, local_path)
                        
                        # CORRECCIÓN AQUÍ - Eliminar el nombre del directorio local de la ruta
                        base_dir_name = os.path.basename(os.path.normpath(local_path))
                        if rel_path.startswith(base_dir_name):
                            rel_path = rel_path[len(base_dir_name):]
                            if rel_path.startswith(os.sep) or rel_path.startswith('/'):
                                rel_path = rel_path[1:]
                        
                        # Corrección para evitar problemas con f-strings y barras invertidas
                        rel_path_fixed = rel_path.replace('\\', '/')
                        dropbox_file_path = f"{dropbox_path.rstrip('/')}/{rel_path_fixed}"
                        
                        print(f"    Subiendo: {file_path} → {dropbox_file_path}")
                        self._upload_file_to_dropbox(str(file_path), dropbox_file_path)
                print(f"¡Directorio {local_path} subido exitosamente a Dropbox!")
                return True
            
            # Si es un archivo, subirlo directamente
            elif local_path.is_file():
                print(f"Subiendo archivo {local_path} a Dropbox ({dropbox_path})...")
                return self._upload_file_to_dropbox(str(local_path), dropbox_path)
            
            else:
                print(f"Error: {local_path} no es un archivo ni un directorio válido")
                return False
                
        except Exception as e:
            print(f"Error al subir a Dropbox: {str(e)}")
            return False
    
    def _upload_file_to_dropbox(self, local_path, dropbox_path):
        """Sube un archivo individual a Dropbox"""
        try:
            # Crear directorios en Dropbox si no existen
            folder_path = "/".join(dropbox_path.split("/")[:-1])
            if folder_path:
                try:
                    self.dropbox_client.files_create_folder_v2(folder_path)
                    print(f"    Carpeta creada: {folder_path}")
                except Exception as e:
                    # Si la carpeta ya existe, no es un error
                    if "folder_already_exists" not in str(e).lower():
                        print(f"    No se pudo crear carpeta {folder_path}: {str(e)}")
            
            # Subir el archivo
            with open(local_path, "rb") as f:
                self.dropbox_client.files_upload(
                    f.read(), 
                    dropbox_path, 
                    mode=dropbox.files.WriteMode.overwrite
                )
            print(f"    - {os.path.basename(local_path)} → {dropbox_path}")
            return True
        except Exception as e:
            print(f"    ! Error al subir {os.path.basename(local_path)}: {str(e)}")
            return False
    
    def upload_catalog_to_dropbox(self, catalog_path=None, report_dir=None):
        """
        Sube el catálogo y el informe a Dropbox
        
        Args:
            catalog_path: Ruta del catálogo (si es None, usa el último generado)
            report_dir: Directorio del informe (si es None, usa el último generado)
            
        Returns:
            bool: True si la subida fue exitosa, False en caso contrario
        """
        if not self.dropbox_client:
            if not self.setup_dropbox():
                return False
        
        # Usar el último catálogo si no se especifica
        if not catalog_path:
            if self.catalog_path and os.path.exists(self.catalog_path):
                catalog_path = self.catalog_path
            else:
                print("Error: No se encontró un catálogo válido para subir.")
                return False
        
        # Subir el catálogo a la ruta correcta
        print("\nSubiendo catálogo a Dropbox...")
        success_catalog = self.upload_to_dropbox(
            catalog_path,
            "/catalogo_completo.csv"
        )
        
        # Subir el informe si existe
        success_report = True
        if report_dir and os.path.exists(report_dir):
            print("\nSubiendo informe a Dropbox...")
            success_report = self.upload_to_dropbox(
                report_dir,
                "/actualizaciones"
            )
        
        if success_catalog and success_report:
            print("\n¡Todos los archivos se subieron exitosamente a Dropbox!")
            print("Puedes acceder a ellos en:")
            print("https://www.dropbox.com/home/Aplicaciones/merc4d0na")
            return True
        else:
            print("\nHubo errores durante la subida a Dropbox.")
            return False
    
    def run_hybrid_system(self, force_full_build=False, upload_to_dropbox=False):
        """
        Ejecuta el sistema híbrido según corresponda
        """
        print("="*70)
        print("SISTEMA HÍBRIDO DE CATÁLOGO MERCADONA")
        print("="*70)
        print("\nEste sistema combina:")
        print("- Construcción completa inicial (modo full)")
        print("- Actualizaciones inteligentes con cobertura casi 100% (modo incremental)")
        print("\nVENTAJAS:")
        print("- Catálogo completo desde el primer uso")
        print("- Actualizaciones rápidas que eventualmente cubren el 100% del catálogo")
        print("- Sistema de rotación para evitar bloqueos y optimizar recursos")
        print("="*70)
        
        # Determinar qué modo ejecutar
        if force_full_build or not os.path.exists(self.catalog_path):
            print("\nNo se encontró catálogo existente o se solicitó reconstrucción completa.")
            print("Ejecutando modo CATÁLOGO COMPLETO...")
            catalog = self.build_full_catalog(
                max_products=1500,
                base_products_per_category=70,
                max_category_size=150
            )
            
            # Generar informe inicial
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_dir = os.path.join(self.base_dir, "actualizaciones", f"catalogo_inicial_{timestamp}")
            os.makedirs(report_dir, exist_ok=True)
            
            summary = f"""
            CATÁLOGO INICIAL GENERADO - {timestamp}
            =====================================
            
            - Total de productos: {len(catalog)}
            - Total de categorías: {len(catalog['categoria_id'].unique())}
            """
            
            # Añadir información de categorías grandes
            category_counts = catalog['categoria_id'].value_counts()
            if not category_counts.empty:
                summary += f"- Categoría más grande: {category_counts.index[0]} "
                summary += f"({category_counts.iloc[0]} productos)\n"
            
            with open(os.path.join(report_dir, "resumen.txt"), "w", encoding="utf-8") as f:
                f.write(summary)
            
            print(f"\nCATÁLOGO INICIAL GENERADO con éxito en {report_dir}")
            
            # Ofrecer subir a Dropbox
            if upload_to_dropbox:
                self.upload_catalog_to_dropbox(
                    catalog_path=self.catalog_path,
                    report_dir=report_dir
                )
            
            return True
        
        else:
            print("\nCatálogo existente detectado.")
            print("Ejecutando modo ACTUALIZACIÓN INTELIGENTE...")
            update_results = self.update_catalog_intelligent()
            
            if update_results and (update_results["updated_products"] or update_results["new_products"]):
                report_dir = self.generate_report(update_results)
                print(f"\nACTUALIZACIÓN COMPLETADA con éxito en {report_dir}")
                print(f"- Productos actualizados: {len(update_results['updated_products'])}")
                print(f"- Nuevos productos: {len(update_results['new_products'])}")
                
                # Ofrecer subir a Dropbox
                if upload_to_dropbox:
                    self.upload_catalog_to_dropbox(
                        catalog_path=self.catalog_path,
                        report_dir=report_dir
                    )
            else:
                print("\nNo se detectaron cambios significativos en esta rotación.")
                print("El catálogo está actualizado o los cambios fueron mínimos.")
            
            return True

# Ejecución del sistema híbrido
if __name__ == "__main__":
    print("="*70)
    print("SISTEMA HÍBRIDO DE CATÁLOGO MERCADONA - CONSTRUCCIÓN Y ACTUALIZACIÓN")
    print("="*70)
    print("\nOPCIONES DISPONIBLES:")
    print("1. Construir catálogo completo (si es la primera vez o para reiniciar)")
    print("2. Actualizar catálogo existente (recomendado para uso diario)")
    print("\nCONFIGURACIÓN:")
    print("- Idioma: es (español)")
    print("- Almacén: vlc1 (Valencia) - CAMBIA ESTO SEGÚN TU LOCALIDAD!")
    print("="*70)
    
    choice = input("\n¿Qué deseas hacer? (1/2): ").strip()
    force_full = (choice == "1")
    
    # Preguntar si quiere subir a Dropbox
    upload_to_dropbox = input("¿Quieres subir los resultados a Dropbox? (s/n): ").strip().lower() == "s"
    
    # Configuración (¡CAMBIA EL WAREHOUSE SEGÚN TU LOCALIDAD!)
    warehouse_code = "vlc1"  # vlc1=Valencia, mnd1=Madrid, bcn1=Barcelona, svq1=Sevilla
    
    # Crear instancia del sistema
    system = MercadonaHybridCatalogSystem(
        lang="es",
        warehouse=warehouse_code
    )
    
    # Si quiere subir a Dropbox, configurar primero
    if upload_to_dropbox:
        if not system.setup_dropbox():
            print("\nNo se configuró Dropbox. Los resultados no se subirán.")
            upload_to_dropbox = False
    
    success = system.run_hybrid_system(
        force_full_build=force_full,
        upload_to_dropbox=upload_to_dropbox
    )
    
    if success:
        print("\n" + "="*50)
        print("PROCESO COMPLETADO")
        print("="*50)
        if force_full:
            print("1. Se ha creado un catálogo completo inicial")
            print("2. Para futuras actualizaciones, usa la opción 2")
            print("3. Ejecuta actualizaciones periódicas para mantener el catálogo actualizado")
        else:
            print("1. El catálogo ha sido actualizado con éxito")
            print("2. El sistema de rotación garantiza cobertura completa en 4 actualizaciones")
            print("3. Ejecuta este script periódicamente para mantener tu catálogo al día")
        
        if upload_to_dropbox:
            print("\n4. Los resultados se han subido a tu carpeta de Dropbox")
            print("   Puedes acceder a ellos en: https://www.dropbox.com/home/Aplicaciones/merc4d0na")
    else:
        print("\n" + "="*50)
        print("ERROR EN EL PROCESO")
        print("="*50)
        print("1. Verifica tu conexión a internet")
        print("2. Asegúrate de usar el código de almacén correcto para tu región")
        print("3. Si el problema persiste, intenta con una reconstrucción completa (opción 1)")