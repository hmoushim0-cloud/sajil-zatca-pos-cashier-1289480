package sa.factory.sajilzatcaposcashi.data.repository

import sa.factory.sajilzatcaposcashi.domain.repository.SajilZatcaRepository
import sa.factory.sajilzatcaposcashi.hardware.HardwareSensorDriver
import sa.factory.sajilzatcaposcashi.model.SajilZatcaItem
import sa.factory.sajilzatcaposcashi.model.SajilZatcaOrder
import sa.factory.sajilzatcaposcashi.model.SajilZatcaTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class SajilZatcaRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : SajilZatcaRepository {

    private val _catalog = MutableStateFlow<List<SajilZatcaItem>>(listOf(
        SajilZatcaItem("1", "حزمة بوستر + فاتورة", "تصميم بوستر إبداعي مع دمج بيانات المنتج وطباعة فاتورة حرارية مخصصة", 220.0, "مدمج", "كيك فاخر"),
        SajilZatcaItem("2", "مسح باركود المخزون", "مزامنة مخزون المنتجات عبر ماسح الباركود مع تحديثات الوقت الفعلي", 180.0, "فوري", "شوكولاتة فاخرة"),
        SajilZatcaItem("3", "مجموعة بوسترات شهرية", "40 تصميم بوستر متجاوب مع منصات التواصل الاجتماعي + قوالب قابلة للتخصيص", 980.0, "جملة", "عروض خاصة")
    ))
    override val catalogFlow: Flow<List<SajilZatcaItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<SajilZatcaItem>>(emptyList())
    override val activeCart: StateFlow<List<SajilZatcaItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<SajilZatcaTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            SajilZatcaTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: SajilZatcaItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: SajilZatcaItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): SajilZatcaOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = SajilZatcaOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
