import pandas as pd
import requests
import json
import os
import time
import random
from datetime import datetime
import sys
import shutil

class MercadonaCatalogGenerator:
    BASE_URL = "https://tienda.mercadona.es"
    CACHE_DIR = "mercadona_cache_v6"
    
    def __init__(self, lang="es", warehouse="vlc1"):
        """
        Inicializa el generador de catálogo simplificado
        Args:
            lang: Código de idioma
            warehouse: Código de almacén/localización
        """
        self.base_dir = self._get_base_directory()
        self.lang = lang
        self.warehouse = warehouse
        self.headers = self._create_headers()
        self._setup_directories()
        
        # Ruta fija para el catálogo actual
        self.catalog_path = os.path.join(self.base_dir, "catalogos", "catalogo_completo_actual.csv")
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print(f"Directorio base configurado: {self.base_dir}")
        print(f"Ruta del catálogo: {os.path.abspath(self.catalog_path)}")
    
    def _get_base_directory(self):
        """Obtiene el directorio base independiente del sistema operativo"""
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
        os.makedirs(os.path.join(self.CACHE_DIR, "products"), exist_ok=True)
        os.makedirs(os.path.join(self.CACHE_DIR, "categories"), exist_ok=True)
        os.makedirs(os.path.join(self.base_dir, "catalogos"), exist_ok=True)
        print("Directorios configurados correctamente:")
        print(f"- Caché: {os.path.abspath(self.CACHE_DIR)}")
        print(f"- Catálogos: {os.path.join(self.base_dir, 'catalogos')}")
    
    def _clean_product_id(self, product_id):
        """Limpia el ID del producto"""
        if isinstance(product_id, float) and product_id.is_integer():
            return str(int(product_id))
        return str(product_id)
    
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
    
    def build_full_catalog(self, max_products=2000, base_products_per_category=80, max_category_size=180):
        """
        Construye un catálogo completo desde cero
        """
        print("\n" + "="*70)
        print("GENERANDO CATÁLOGO COMPLETO DE MERCADONA")
        print("="*70)
        
        # Semillas estratégicas predefinidas
        strategic_seeds = [
            "3497", "86385", "21329", "60091", "84785", "52710", "62048", "40229", 
            "86397", "30167", "3819", "23017", "23013", "35420", "18086", "86905", 
            "86786", "9264", "13204", "66462", "9280", "19897", "5044", "22910", 
            "28035", "4241"
        ]
        
        all_products = []
        products_by_category = {}
        visited = set()
        
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
                        
                        # Actualizar conteo por categoría
                        if cat_id not in products_by_category:
                            products_by_category[cat_id] = 0
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
        
        # Crear DataFrame y guardar
        if all_products:
            catalog_df = pd.DataFrame(all_products)
            
            # Guardar catálogo completo en la ruta fija
            os.makedirs(os.path.dirname(self.catalog_path), exist_ok=True)
            catalog_df.to_csv(self.catalog_path, index=False, encoding="utf-8-sig")
            
            # Crear archivo de marca para indicar éxito
            with open(os.path.join(self.base_dir, "catalogos", "build_successful.txt"), "w") as f:
                f.write(f"Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total de productos: {len(all_products)}\n")
                f.write(f"Categorías: {len(products_by_category)}\n")
            
            # Mostrar análisis de categorías
            print("\n" + "="*50)
            print("ANÁLISIS DEL CATÁLOGO")
            print("="*50)
            print(f"Total de productos: {len(all_products)}")
            print(f"Total de categorías: {len(products_by_category)}")
            
            # Mostrar las 5 categorías más grandes
            sorted_categories = sorted(products_by_category.items(), key=lambda x: x[1], reverse=True)
            print("\nTOP 5 CATEGORÍAS MÁS GRANDES:")
            for i, (cat_id, count) in enumerate(sorted_categories[:5], 1):
                try:
                    # Buscar el nombre de la categoría en los productos
                    cat_name = next(p["categoria"] for p in all_products if p["categoria_id"] == cat_id)
                    print(f"{i}. {cat_name}: {count} productos")
                except:
                    print(f"{i}. Categoría ID {cat_id}: {count} productos")
            
            print(f"\nCATÁLOGO GENERADO CON ÉXITO: {len(all_products)} productos")
            print(f"Archivo guardado como: {os.path.abspath(self.catalog_path)}")
            return catalog_df
        
        print("\n❌ No se pudieron obtener productos. Verifica tu conexión o el código de almacén.")
        return None

if __name__ == "__main__":
    print("="*70)
    print("GENERADOR DIARIO DE CATÁLOGO MERCADONA")
    print("="*70)
    
    # Configuración desde variables de entorno (para GitHub Actions)
    warehouse_code = os.getenv('WAREHOUSE_CODE', 'vlc1')  # Valor por defecto para Valencia
    max_products = int(os.getenv('MAX_PRODUCTS', '2000'))
    
    print(f"\nConfiguración de ejecución:")
    print(f"- Código de almacén: {warehouse_code}")
    print(f"- Máximo de productos: {max_products}")
    
    # Crear instancia del sistema
    generator = MercadonaCatalogGenerator(
        lang="es",
        warehouse=warehouse_code
    )
    
    # Generar catálogo completo
    catalog = generator.build_full_catalog(max_products=max_products)
    
    if catalog is not None:
        print("\n✅ Proceso completado con éxito")
        sys.exit(0)
    else:
        print("\n❌ Error durante la generación del catálogo")
        sys.exit(1)
