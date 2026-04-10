package com.example.pricewise.feature.search.domain.model

enum class SearchSortOrder {
    PRICE_ASC,
    PRICE_DESC,
    RELEVANCE,
}

enum class DeliveryFilter {
    NONE,
    TODAY,
    TODAY_TOMORROW,
    UP_TO_7_DAYS,
}

data class SearchFilters(
    val sortOrder: SearchSortOrder = SearchSortOrder.PRICE_ASC,
    val priceMin: Long? = null,
    val priceMax: Long? = null,
    val deliveryFilter: DeliveryFilter = DeliveryFilter.NONE,
    val onlyOriginal: Boolean = false,
    val onlyNew: Boolean = false,
    val onlyUsed: Boolean = false,
    val marketplaceOnly: Boolean = false,
    val offlineOnly: Boolean = false,
    val payLaterOnly: Boolean = false,
) {
    fun hasProductFilters(): Boolean {
        return onlyOriginal ||
            onlyNew ||
            onlyUsed ||
            marketplaceOnly ||
            offlineOnly ||
            payLaterOnly ||
            deliveryFilter != DeliveryFilter.NONE
    }

    fun hasPriceFilters(): Boolean {
        return sortOrder != SearchSortOrder.PRICE_ASC || priceMin != null || priceMax != null
    }
}
