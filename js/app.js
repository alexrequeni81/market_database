document.addEventListener('DOMContentLoaded', function() {
    const productsTable = document.getElementById('products-table').querySelector('tbody');
    const searchInput = document.getElementById('search-input');
    const categoryFilter = document.getElementById('category-filter');
    const refreshBtn = document.getElementById('refresh-btn');
    const totalProducts = document.getElementById('total-products');
    const totalCategories = document.getElementById('total-categories');
    const updateDate = document.getElementById('update-date');
    const lastUpdate = document.getElementById('last-update');
    const loading = document.getElementById('loading');
    const errorMessage = document.getElementById('error-message');
    
    let products = [];
    let categories = new Set();
    
    // Ruta al CSV (relativa a la raíz del sitio)
    const csvPath = '/catalogos/catalogo_completo_actual.csv';
    const buildInfoPath = '/catalogos/build_successful.txt';
    
    // Cargar los datos
    function loadData() {
        loading.style.display = 'block';
        errorMessage.style.display = 'none';
        
        // Primero obtener información de actualización
        fetch(buildInfoPath)
            .then(response => {
                if (!response.ok) throw new Error('No se pudo cargar la información de actualización');
                return response.text();
            })
            .then(text => {
                const lines = text.split('\n');
                const updateLine = lines.find(line => line.includes('Última actualización'));
                const productsLine = lines.find(line => line.includes('Total de productos'));
                
                if (updateLine) {
                    const date = updateLine.split(':')[1].trim();
                    updateDate.textContent = date;
                    lastUpdate.textContent = `Última actualización: ${date}`;
                }
                
                if (productsLine) {
                    const count = productsLine.split(':')[1].trim();
                    totalProducts.textContent = count;
                }
            })
            .catch(error => {
                console.error('Error al cargar info de actualización:', error);
                updateDate.textContent = 'Error al cargar';
            });
        
        // Cargar el CSV
        Papa.parse(csvPath, {
            download: true,
            header: true,
            dynamicTyping: true,
            complete: function(results) {
                loading.style.display = 'none';
                
                if (results.errors.length > 0) {
                    showError(`Error al procesar el CSV: ${results.errors[0].message}`);
                    return;
                }
                
                products = results.data.filter(product => 
                    product.id && product.nombre && product.categoria
                );
                
                // Obtener categorías únicas
                categories = new Set();
                products.forEach(product => {
                    if (product.categoria) {
                        categories.add(product.categoria);
                    }
                });
                
                // Actualizar estadísticas
                totalProducts.textContent = products.length;
                totalCategories.textContent = categories.size;
                
                // Actualizar filtros
                updateCategoryFilters();
                
                // Mostrar productos
                renderProducts(products);
            },
            error: function(error) {
                loading.style.display = 'none';
                showError(`Error al cargar el catálogo: ${error.message}`);
            }
        });
    }
    
    function updateCategoryFilters() {
        // Limpiar opciones existentes (excepto la primera)
        while (categoryFilter.options.length > 1) {
            categoryFilter.remove(1);
        }
        
        // Añadir nuevas opciones
        Array.from(categories).sort().forEach(category => {
            const option = document.createElement('option');
            option.value = category;
            option.textContent = category;
            categoryFilter.appendChild(option);
        });
    }
    
    function renderProducts(productsToRender) {
        productsTable.innerHTML = '';
        
        if (productsToRender.length === 0) {
            const row = document.createElement('tr');
            row.innerHTML = `<td colspan="5" style="text-align: center;">No se encontraron productos</td>`;
            productsTable.appendChild(row);
            return;
        }
        
        productsToRender.forEach(product => {
            const row = document.createElement('tr');
            
            // Formatear precios
            const price = product.precio_total ? parseFloat(product.precio_total).toFixed(2) : 'N/A';
            const unitPrice = product.precio_por_unidad ? 
                `${parseFloat(product.precio_por_unidad).toFixed(2)} ${product.unidad_medida || ''}` : 'N/A';
            
            row.innerHTML = `
                <td>
                    <div class="product-name">${product.nombre || 'Sin nombre'}</div>
                    ${product.url ? `<a href="${product.url}" target="_blank" class="product-link">Ver en Mercadona</a>` : ''}
                </td>
                <td>${product.categoria || 'Sin categoría'}</td>
                <td>${price} €</td>
                <td>${unitPrice}</td>
                <td>${product.disponible === 'True' ? '✅' : '❌'}</td>
            `;
            
            productsTable.appendChild(row);
        });
    }
    
    function filterProducts() {
        const searchTerm = searchInput.value.toLowerCase();
        const selectedCategory = categoryFilter.value;
        
        let filteredProducts = products;
        
        // Filtrar por búsqueda
        if (searchTerm) {
            filteredProducts = filteredProducts.filter(product => 
                product.nombre.toLowerCase().includes(searchTerm) ||
                (product.categoria && product.categoria.toLowerCase().includes(searchTerm))
            );
        }
        
        // Filtrar por categoría
        if (selectedCategory) {
            filteredProducts = filteredProducts.filter(product => 
                product.categoria === selectedCategory
            );
        }
        
        renderProducts(filteredProducts);
    }
    
    function showError(message) {
        errorMessage.textContent = message;
        errorMessage.style.display = 'block';
    }
    
    // Event listeners
    searchInput.addEventListener('input', filterProducts);
    categoryFilter.addEventListener('change', filterProducts);
    
    refreshBtn.addEventListener('click', function() {
        this.disabled = true;
        this.textContent = 'Actualizando...';
        
        loadData();
        
        setTimeout(() => {
            refreshBtn.disabled = false;
            refreshBtn.textContent = 'Actualizar datos';
        }, 1000);
    });
    
    // Cargar datos al iniciar
    loadData();
});
