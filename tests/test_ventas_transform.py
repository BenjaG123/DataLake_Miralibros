"""Pruebas de transformación del pipeline de ventas cabecera/detalle."""

from __future__ import annotations

import logging
from collections import Counter
from datetime import date, datetime, time, timezone
from pathlib import Path

import pandas as pd
import pytest

from etl.pipelines import ventas
from etl.pipelines.ventas import (
    LINE_COLUMNS,
    TICKET_COLUMNS,
    Client,
    PriceCatalog,
    Ticket,
    _leer_ventas,
    _line_rejection,
    assign_clients,
    build_lines,
    compute_reachable,
    deduplicate,
    generar_hora_simulada,
    inferir_origen,
    load_cafeteria_catalog,
    load_clientes,
    load_stock_catalog,
    normalize_canal,
    normalize_si_no,
    reconstruct_prices,
    transform,
    transform_row,
)

LOADED_AT = datetime(2025, 1, 1, tzinfo=timezone.utc)
SOURCE_FILE = "VENTAS LIBRERIA.xlsx"
REGLA_LINEA = "VENTAS-LINEA-001"


def _fila_valida() -> dict[str, object]:
    return {
        "Correlativo": "1",
        "Orden Web": "",
        "Fecha": "2025-01-02",
        "Medio de Pago": "Efectivo",
        "Tipo Documento": "Boleta",
        "Folio": "1000",
        "Voucher Máquina de Pago": "",
        "Total Venta": "1.500",
        "Cant. Items": "2",
        "Canal": "Local",
        "Feria": "",
        "Tiene Descuento": "NO",
        "Es Dev.": "NO",
        "Estado": "Emitida",
    }


def _transformar(
    cambios: dict[str, object],
    *,
    origen: str = "LIBRERIA",
) -> tuple[Ticket | None, str | None]:
    fila = _fila_valida()
    fila.update(cambios)
    return transform_row(
        fila,
        origen=origen,
        source_file=SOURCE_FILE,
        loaded_at=LOADED_AT,
    )


def _ticket(
    *,
    correlativo: int = 1,
    total: int = 1000,
    cant_items: int = 1,
    fecha: date = date(2025, 1, 2),
    origen: str = "LIBRERIA",
) -> Ticket:
    return Ticket(
        origen=origen,
        correlativo=correlativo,
        fecha=fecha,
        hora_simulada=None,
        medio_pago="Efectivo",
        tipo_documento="Boleta",
        folio=str(1000 + correlativo),
        total_venta=total,
        cant_items=cant_items,
        tiene_descuento="NO",
        es_devolucion="NO",
        estado="Emitida",
        source_file=SOURCE_FILE,
        loaded_at=LOADED_AT,
        fila=2,
    )


class TestInferirOrigen:
    """Procedencia a partir del nombre del archivo."""

    @pytest.mark.parametrize(
        ("nombre", "esperado"),
        [
            ("VENTAS LIBRERIA.xlsx", "LIBRERIA"),
            ("VENTAS CAFETERIA.xlsx", "CAFETERIA"),
            ("VENTAS CAFETERÍA.xlsx", "CAFETERIA"),
        ],
    )
    def test_infiere_origen(self, nombre: str, esperado: str) -> None:
        assert inferir_origen(nombre) == esperado

    def test_nombre_sin_origen_falla(self) -> None:
        with pytest.raises(ValueError):
            inferir_origen("VENTAS.xlsx")


class TestNormalizadores:
    """Normalización de canal y flags SI/NO."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("local", "Local"),
            ("LOCAL", "Local"),
            ("web", "Web"),
            ("Web", "Web"),
        ],
    )
    def test_normalize_canal_valido(
        self, crudo: str, esperado: str
    ) -> None:
        assert normalize_canal(crudo) == esperado

    @pytest.mark.parametrize("crudo", ["", None, "sin dato", "otro"])
    def test_normalize_canal_invalido(self, crudo: object) -> None:
        assert normalize_canal(crudo) is None

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("SI", "SI"),
            ("SÍ", "SI"),
            ("S", "SI"),
            ("NO", "NO"),
            ("N", "NO"),
            ("si", "SI"),
            ("no", "NO"),
        ],
    )
    def test_normalize_si_no_valido(
        self, crudo: str, esperado: str
    ) -> None:
        assert normalize_si_no(crudo) == esperado

    @pytest.mark.parametrize("crudo", ["", None, "sin dato", "tal vez"])
    def test_normalize_si_no_invalido(self, crudo: object) -> None:
        assert normalize_si_no(crudo) is None


class TestTransformRow:
    """Normalización de una cabecera y motivos de descarte."""

    def test_fila_valida(self) -> None:
        ticket, error = _transformar({})

        assert error is None
        assert ticket is not None
        assert ticket.origen == "LIBRERIA"
        assert ticket.correlativo == 1
        assert ticket.fecha == date(2025, 1, 2)
        assert isinstance(ticket.hora_simulada, time)
        assert ticket.medio_pago == "Efectivo"
        assert ticket.tipo_documento == "Boleta"
        assert ticket.folio == "1000"
        assert ticket.total_venta == 1500
        assert ticket.cant_items == 2
        assert ticket.tiene_descuento == "NO"
        assert ticket.es_devolucion == "NO"
        assert ticket.estado == "Emitida"
        assert ticket.source_file == SOURCE_FILE
        assert ticket.loaded_at == LOADED_AT
        assert ticket.fila == 0

    def test_to_db_row_respeta_el_orden_de_columnas(self) -> None:
        ticket, _ = _transformar({})
        assert ticket is not None

        fila = ticket.to_db_row("12345678-5")

        assert len(fila) == len(TICKET_COLUMNS)
        assert fila[:5] == (
            "LIBRERIA",
            1,
            date(2025, 1, 2),
            ticket.hora_simulada,
            "12345678-5",
        )
        assert fila[-2:] == (LOADED_AT, LOADED_AT)

    @pytest.mark.parametrize(
        ("cambios", "motivo"),
        [
            ({"Fecha": "31/01/2025"}, "fecha inválida"),
            (
                {"Total Venta": "-1"},
                "total venta inválido o negativo",
            ),
            ({"Cant. Items": "0"}, "cantidad de ítems inválida"),
            ({"Cant. Items": "abc"}, "cantidad de ítems inválida"),
            ({"Correlativo": "0"}, "correlativo inválido"),
        ],
    )
    def test_fila_invalida(
        self, cambios: dict[str, object], motivo: str
    ) -> None:
        ticket, error = _transformar(cambios)

        assert ticket is None
        assert error is not None
        assert motivo in error


class TestContratoDeColumnas:
    """Header crudo real, columnas descartadas y columnas opcionales."""

    COLUMNAS_CRUDAS = (
        "Correlativo",
        "Orden Web",
        "Fecha",
        "Medio de Pago",
        "Tipo Documento",
        "Folio",
        "Voucher Máquina de Pago",
        "Total Venta",
        "Cant. Items",
        "Canal",
        "Feria",
        "Tiene Descuento",
        "Estado",
        "Es Dev.",
    )

    MINIMAS = (
        "Correlativo",
        "Fecha",
        "Tipo Documento",
        "Folio",
        "Total Venta",
        "Cant. Items",
    )

    def test_source_columns_es_el_header_real(self) -> None:
        assert ventas.SOURCE_COLUMNS == self.COLUMNAS_CRUDAS

    def test_leer_ventas_elimina_columnas_descartadas(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        completo = pd.DataFrame(
            [{columna: "" for columna in ventas.SOURCE_COLUMNS}]
        )
        monkeypatch.setattr(
            ventas,
            "read_excel_as_str",
            lambda _path: completo,
        )

        dataframe = _leer_ventas("VENTAS LIBRERIA.xlsx")

        assert "Voucher Máquina de Pago" not in dataframe.columns
        assert "Feria" not in dataframe.columns
        assert tuple(dataframe.columns) == tuple(
            columna
            for columna in ventas.SOURCE_COLUMNS
            if columna not in ventas.DROPPED_COLUMNS
        )

    def test_leer_ventas_tolera_opcionales_ausentes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        minimo = pd.DataFrame(
            [{columna: "" for columna in self.MINIMAS}]
        )
        monkeypatch.setattr(
            ventas,
            "read_excel_as_str",
            lambda _path: minimo,
        )

        dataframe = _leer_ventas("VENTAS LIBRERIA.xlsx")

        assert tuple(dataframe.columns) == self.MINIMAS

    def test_origen_extra_no_se_exige_ni_llega_al_ticket(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fila = {
            "Correlativo": "1",
            "Fecha": "2025-01-02",
            "Tipo Documento": "Boleta",
            "Folio": "1000",
            "Total Venta": "1500",
            "Cant. Items": "2",
            "Origen": "CAFETERIA",
        }
        monkeypatch.setattr(
            ventas,
            "read_excel_as_str",
            lambda _path: pd.DataFrame([fila]),
        )

        dataframe = _leer_ventas("VENTAS LIBRERIA.xlsx")
        tickets, rechazos = ventas.transform(
            dataframe,
            origen="LIBRERIA",
            source_file=SOURCE_FILE,
            loaded_at=LOADED_AT,
        )

        assert "Origen" in dataframe.columns
        assert "Origen" not in ventas.TICKET_COLUMNS
        assert rechazos == []
        assert len(tickets) == 1
        assert tickets[0].origen == "LIBRERIA"

    def test_transform_sin_opcionales_no_lanza_keyerror(self) -> None:
        fila = {
            "Correlativo": "1",
            "Fecha": "2025-01-02",
            "Tipo Documento": "Boleta",
            "Folio": "1000",
            "Total Venta": "1500",
            "Cant. Items": "2",
        }

        tickets, rechazos = ventas.transform(
            pd.DataFrame([fila]),
            origen="LIBRERIA",
            source_file=SOURCE_FILE,
            loaded_at=LOADED_AT,
        )
        desde_dict, error = ventas.transform_row(
            fila,
            origen="LIBRERIA",
            source_file=SOURCE_FILE,
            loaded_at=LOADED_AT,
        )

        assert rechazos == []
        assert len(tickets) == 1
        assert error is None
        assert desde_dict is not None
        for ticket in (tickets[0], desde_dict):
            assert ticket.medio_pago is None
            assert ticket.tiene_descuento is None
            assert ticket.es_devolucion is None
            assert ticket.estado is None


class TestReglasDeRechazo:
    """Folio y tipo de documento tienen reglas y motivos propios."""

    @pytest.mark.parametrize("folio", ["0", "000", "", None])
    def test_folio_invalido_rechaza(self, folio: object) -> None:
        ticket, error = _transformar({"Folio": folio})

        assert ticket is None
        assert error is not None
        assert "folio inválido o vacío" in error

    @pytest.mark.parametrize(
        "tipo_documento",
        ["Sin Documento", "sin documento", "  SIN   DOCUMENTO "],
    )
    def test_tipo_documento_sin_documento_rechaza(
        self, tipo_documento: str
    ) -> None:
        ticket, error = _transformar(
            {"Tipo Documento": tipo_documento}
        )

        assert ticket is None
        assert error is not None
        assert "tipo documento sin documento" in error

    def test_varios_motivos_se_unen(self) -> None:
        ticket, error = _transformar(
            {"Folio": "0", "Tipo Documento": "Sin Documento"}
        )

        assert ticket is None
        assert error is not None
        assert "folio inválido o vacío" in error
        assert "tipo documento sin documento" in error


class TestHoraSimulada:
    """Ventana y determinismo de la hora sintética por ticket."""

    def test_dia_de_semana_entre_10_y_1859(self) -> None:
        ticket, error = _transformar({"Fecha": "2024-01-01"})

        assert error is None
        assert ticket is not None
        assert isinstance(ticket.hora_simulada, time)
        assert time(10, 0) <= ticket.hora_simulada <= time(18, 59)

    def test_sabado_entre_10_y_1759(self) -> None:
        ticket, error = _transformar({"Fecha": "2024-01-06"})

        assert error is None
        assert ticket is not None
        assert isinstance(ticket.hora_simulada, time)
        assert time(10, 0) <= ticket.hora_simulada <= time(17, 59)

    def test_domingo_queda_none_y_advierte(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            ticket, error = _transformar({"Fecha": "2024-01-07"})

        assert error is None
        assert ticket is not None
        assert ticket.hora_simulada is None
        assert "hora_simulada NULL" in caplog.text

    def test_es_deterministica(self) -> None:
        fecha = date(2024, 1, 2)

        primera = generar_hora_simulada("CAFETERIA", 1001, fecha)
        segunda = generar_hora_simulada("CAFETERIA", 1001, fecha)

        assert primera is not None
        assert primera == segunda

    def test_distribucion_por_origen(self) -> None:
        fecha = date(2024, 1, 2)
        correlativos = range(1, 801)

        def contar(origen: str) -> tuple[int, int]:
            mediodia = tarde = 0
            for correlativo in correlativos:
                hora = generar_hora_simulada(origen, correlativo, fecha)
                assert hora is not None
                if time(12, 0) <= hora < time(14, 0):
                    mediodia += 1
                if time(17, 0) <= hora < time(19, 0):
                    tarde += 1
            return mediodia, tarde

        cafeteria_mediodia, cafeteria_tarde = contar("CAFETERIA")
        libreria_mediodia, libreria_tarde = contar("LIBRERIA")

        assert cafeteria_mediodia > cafeteria_tarde
        assert libreria_tarde > libreria_mediodia


class TestTransformLote:
    """Contrato de volumen y calidad sobre los fixtures sintéticos."""

    def test_libreria_respeta_el_contrato(
        self, ventas_libreria_df: pd.DataFrame
    ) -> None:
        tickets, rejected = transform(
            ventas_libreria_df,
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
        )

        assert len(tickets) == 137
        assert len(rejected) == 18
        assert len(tickets) + len(rejected) == 155

    def test_cafeteria_respeta_el_contrato(
        self, ventas_cafeteria_df: pd.DataFrame
    ) -> None:
        tickets, rejected = transform(
            ventas_cafeteria_df,
            origen="CAFETERIA",
            source_file="VENTAS CAFETERIA.xlsx",
        )

        assert len(tickets) == 70
        assert len(rejected) == 15
        assert len(tickets) + len(rejected) == 85

    def test_motivos_de_descarte_coinciden(
        self,
        ventas_libreria_df: pd.DataFrame,
        ventas_cafeteria_df: pd.DataFrame,
    ) -> None:
        _, libreria = transform(
            ventas_libreria_df,
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
        )
        _, cafeteria = transform(
            ventas_cafeteria_df,
            origen="CAFETERIA",
            source_file="VENTAS CAFETERIA.xlsx",
        )

        motivos = Counter(
            motivo
            for item in [*libreria, *cafeteria]
            for motivo in item["motivo"].split("; ")
        )
        assert motivos == Counter(
            {
                "total venta inválido o negativo": 12,
                "cantidad de ítems inválida": 11,
                "folio inválido o vacío": 6,
                "tipo documento sin documento": 6,
            }
        )

    def test_filas_validas_cumplen_el_contrato(
        self,
        ventas_libreria_df: pd.DataFrame,
        ventas_cafeteria_df: pd.DataFrame,
    ) -> None:
        libreria, _ = transform(
            ventas_libreria_df,
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
        )
        cafeteria, _ = transform(
            ventas_cafeteria_df,
            origen="CAFETERIA",
            source_file="VENTAS CAFETERIA.xlsx",
        )

        assert len(libreria) + len(cafeteria) == 207
        for ticket in [*libreria, *cafeteria]:
            assert len(ticket.to_db_row(None)) == len(TICKET_COLUMNS)
            assert ticket.origen in ("LIBRERIA", "CAFETERIA")
            assert ticket.correlativo > 0
            assert ticket.fecha
            assert ticket.hora_simulada is None or isinstance(
                ticket.hora_simulada, time
            )
            assert ticket.total_venta >= 0
            assert ticket.cant_items > 0
            assert ticket.tiene_descuento in (None, "SI", "NO")
            assert ticket.es_devolucion in (None, "SI", "NO")


class TestDeduplicate:
    """Deduplicación por ``(origen, correlativo)``."""

    def test_conserva_la_ultima_fila(self) -> None:
        primero, error = _transformar({"Total Venta": "1000"})
        segundo, error_segundo = _transformar({"Total Venta": "2000"})
        assert error is None and error_segundo is None
        assert primero is not None and segundo is not None

        rows, duplicates = deduplicate([primero, segundo])

        assert duplicates == 1
        assert len(rows) == 1
        assert rows[0].total_venta == 2000

    def test_mismo_correlativo_con_otro_origen_no_es_duplicado(self) -> None:
        libreria, _ = _transformar({}, origen="LIBRERIA")
        cafeteria, _ = _transformar({}, origen="CAFETERIA")
        assert libreria is not None and cafeteria is not None

        rows, duplicates = deduplicate([libreria, cafeteria])

        assert duplicates == 0
        assert len(rows) == 2


class TestCatalogos:
    """Carga de catálogos y pertenencia de los códigos internos."""

    def test_load_stock_catalog(
        self, archivos_fixtures: dict[str, Path]
    ) -> None:
        catalog = load_stock_catalog(
            archivos_fixtures["stock"],
            loaded_at=LOADED_AT,
        )

        assert catalog.prices
        assert list(catalog.prices) == sorted(catalog.prices)
        assert min(catalog.prices) >= 100
        for precio, codigos in catalog.codes_by_price.items():
            assert codigos
            assert all(len(codigo) == 13 for codigo in codigos)
            assert precio in catalog.prices

    def test_load_cafeteria_catalog(
        self, archivos_fixtures: dict[str, Path]
    ) -> None:
        catalog = load_cafeteria_catalog(
            archivos_fixtures["catalogo_cafeteria"]
        )

        assert 2500 in catalog.prices
        assert 3000 in catalog.prices
        assert 5500 in catalog.prices
        assert set(catalog.codes_by_price[3000]) >= {
            "ESP-001",
            "LEC-001",
            "INF-001",
            "PAS-001",
            "PAS-002",
            "PAS-004",
        }


class TestComputeReachable:
    """Bitsets de sumas alcanzables con exactamente ``k`` precios."""

    PRECIOS = (1000, 2000, 3000)

    def _reachable(self) -> list[int]:
        return compute_reachable(self.PRECIOS, 3, 6000)

    @pytest.mark.parametrize(
        ("cantidad", "sumas"),
        [
            (0, [0]),
            (1, [1000, 2000, 3000]),
            (2, [2000, 3000, 4000, 5000, 6000]),
            (3, [3000, 4000, 5000, 6000]),
        ],
    )
    def test_sumas_alcanzables(
        self, cantidad: int, sumas: list[int]
    ) -> None:
        reachable = self._reachable()

        for suma in sumas:
            assert (reachable[cantidad] >> suma) & 1, (
                f"{suma} no es alcanzable con {cantidad} precios"
            )

    def test_reconstruct_prices_exacto(self) -> None:
        reachable = self._reachable()

        primera = reconstruct_prices(6000, 3, self.PRECIOS, reachable)
        segunda = reconstruct_prices(6000, 3, self.PRECIOS, reachable)

        assert primera is not None
        assert len(primera) == 3
        assert sum(primera) == 6000
        assert primera == segunda

    @pytest.mark.parametrize(
        ("total", "cantidad"),
        [(5500, 2), (1000, 2), (7000, 2)],
    )
    def test_reconstruct_prices_sin_combinacion(
        self, total: int, cantidad: int
    ) -> None:
        assert (
            reconstruct_prices(
                total,
                cantidad,
                self.PRECIOS,
                self._reachable(),
            )
            is None
        )

    def test_reconstruct_prices_cero(self) -> None:
        reachable = self._reachable()

        assert reconstruct_prices(0, 0, self.PRECIOS, reachable) == []
        assert reconstruct_prices(1, 0, self.PRECIOS, reachable) is None


class TestBuildLines:
    """Líneas exactas, códigos internos y cuadre de la cabecera."""

    PRECIOS = (1000, 2000)

    def _catalogo(self) -> PriceCatalog:
        return PriceCatalog(
            prices=self.PRECIOS,
            codes_by_price={1000: ("A",), 2000: ("B",)},
        )

    def test_lineas_cuadran_y_codigos_pertenecen(self) -> None:
        catalog = self._catalogo()
        ticket = _ticket(total=3000, cant_items=2)
        reachable = compute_reachable(self.PRECIOS, 2, 3000)

        lines = build_lines(
            ticket,
            catalog,
            reachable,
            loaded_at=LOADED_AT,
        )

        assert lines is not None
        assert sum(line.cantidad for line in lines) == ticket.cant_items
        assert sum(line.total_linea for line in lines) == ticket.total_venta
        assert [line.linea for line in lines] == list(
            range(1, len(lines) + 1)
        )
        for line in lines:
            assert line.origen == ticket.origen
            assert line.correlativo == ticket.correlativo
            assert line.precio_unitario in catalog.prices
            assert line.codigo_interno in catalog.codes_by_price[
                line.precio_unitario
            ]
            assert line.total_linea == line.cantidad * line.precio_unitario
            assert len(line.to_db_row()) == len(LINE_COLUMNS)

    def test_sin_combinacion_retorna_none(self) -> None:
        catalog = self._catalogo()
        ticket = _ticket(total=5500, cant_items=2)
        reachable = compute_reachable(self.PRECIOS, 2, 5500)

        assert (
            build_lines(
                ticket,
                catalog,
                reachable,
                loaded_at=LOADED_AT,
            )
            is None
        )

    def test_codigo_de_libreria_pertenece_al_stock(
        self, archivos_fixtures: dict[str, Path]
    ) -> None:
        catalog = load_stock_catalog(
            archivos_fixtures["stock"],
            loaded_at=LOADED_AT,
        )
        precio = catalog.prices[0]
        ticket = _ticket(total=precio, cant_items=1)
        reachable = compute_reachable(catalog.prices, 1, precio)

        lines = build_lines(
            ticket,
            catalog,
            reachable,
            loaded_at=LOADED_AT,
        )

        assert lines is not None
        assert len(lines) == 1
        assert lines[0].codigo_interno in catalog.codes_by_price[precio]

    def test_codigo_de_cafeteria_pertenece_al_catalogo(
        self, archivos_fixtures: dict[str, Path]
    ) -> None:
        catalog = load_cafeteria_catalog(
            archivos_fixtures["catalogo_cafeteria"]
        )
        precio = catalog.prices[0]
        ticket = _ticket(
            total=precio,
            cant_items=1,
            origen="CAFETERIA",
        )
        reachable = compute_reachable(catalog.prices, 1, precio)

        lines = build_lines(
            ticket,
            catalog,
            reachable,
            loaded_at=LOADED_AT,
        )

        assert lines is not None
        assert len(lines) == 1
        assert lines[0].codigo_interno in catalog.codes_by_price[precio]

    def test_line_rejection_usa_la_regla_de_cuarentena(self) -> None:
        ticket = _ticket(total=5500, cant_items=2)

        rechazo = _line_rejection(ticket)

        assert rechazo["regla"] == REGLA_LINEA
        assert rechazo["motivo"] == "sin combinación exacta de líneas"
        assert rechazo["columna"] == "total_venta"
        assert rechazo["fila"] == ticket.fila
        assert rechazo["detalles"][0]["regla"] == REGLA_LINEA


class TestAssignClients:
    """Asignación de RUT por subset sum exacto."""

    def _clientes(
        self,
        *definiciones: tuple[str, date | None, int],
    ) -> list[Client]:
        return [
            Client(rut=rut, alta=alta, compras=compras)
            for rut, alta, compras in definiciones
        ]

    def test_asigna_subconjunto_exacto(self) -> None:
        tickets = [
            _ticket(correlativo=1, total=1000),
            _ticket(correlativo=2, total=2000),
        ]
        clients = self._clientes(
            ("11111111-1", date(2024, 1, 1), 3000),
        )

        assignments = assign_clients(tickets, clients)

        assert set(assignments) == {("LIBRERIA", 1), ("LIBRERIA", 2)}
        assert assignments[("LIBRERIA", 1)] == "11111111-1"

    def test_asigna_cliente_con_dos_tickets(self) -> None:
        tickets = [
            _ticket(correlativo=1, total=5000),
            _ticket(correlativo=2, total=3000),
            _ticket(correlativo=3, total=1000),
        ]
        clients = self._clientes(
            ("22222222-2", date(2024, 1, 1), 8000),
        )

        assignments = assign_clients(tickets, clients)

        assert len(assignments) == 2
        assert set(assignments) == {("LIBRERIA", 1), ("LIBRERIA", 2)}

    def test_restriccion_temporal(self) -> None:
        tickets = [
            _ticket(
                correlativo=1,
                total=1000,
                fecha=date(2023, 12, 31),
            ),
            _ticket(
                correlativo=2,
                total=2000,
                fecha=date(2024, 1, 2),
            ),
        ]
        clients = self._clientes(
            ("33333333-3", date(2024, 1, 1), 2000),
        )

        assignments = assign_clients(tickets, clients)

        assert assignments == {("LIBRERIA", 2): "33333333-3"}

    def test_deja_tickets_anonimos(self) -> None:
        tickets = [
            _ticket(correlativo=1, total=1000),
            _ticket(correlativo=2, total=2000),
        ]
        clients = self._clientes(
            ("44444444-4", date(2024, 1, 1), 1000),
        )

        assignments = assign_clients(tickets, clients)

        assert assignments == {("LIBRERIA", 1): "44444444-4"}
        assert ("LIBRERIA", 2) not in assignments

    def test_cliente_sin_subconjunto_advierte(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        tickets = [_ticket(correlativo=1, total=1000)]
        clients = self._clientes(
            ("55555555-5", date(2024, 1, 1), 500),
        )

        with caplog.at_level(logging.WARNING):
            assignments = assign_clients(tickets, clients)

        assert assignments == {}
        assert "Sin subconjunto exacto" in caplog.text

    def test_no_reutiliza_un_ticket(self) -> None:
        tickets = [_ticket(correlativo=1, total=1000)]
        clients = self._clientes(
            ("66666666-6", date(2024, 1, 1), 1000),
            ("77777777-7", date(2024, 1, 1), 1000),
        )

        assignments = assign_clients(tickets, clients)

        assert len(assignments) == 1
        assert assignments[("LIBRERIA", 1)] == "66666666-6"

    def test_cliente_sin_alta_no_se_asigna(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        tickets = [_ticket(correlativo=1, total=1000)]
        clients = self._clientes(("88888888-8", None, 1000))

        with caplog.at_level(logging.WARNING):
            assignments = assign_clients(tickets, clients)

        assert assignments == {}
        assert "sin alta válida" in caplog.text

    def test_cliente_compras_cero_no_se_asigna(self) -> None:
        tickets = [_ticket(correlativo=1, total=1000)]
        clients = self._clientes(
            ("99999999-9", date(2024, 1, 1), 0),
        )

        assert assign_clients(tickets, clients) == {}

    def test_es_deterministica(self) -> None:
        tickets = [
            _ticket(correlativo=1, total=3000),
            _ticket(correlativo=2, total=2000),
            _ticket(correlativo=3, total=1000),
        ]
        clients = self._clientes(
            ("10101010-1", date(2024, 1, 1), 3000),
            ("20202020-2", date(2024, 1, 1), 2000),
        )

        primera = assign_clients(tickets, clients)
        segunda = assign_clients(tickets, clients)

        assert primera == segunda


class TestLoadClientes:
    """Clientes en memoria con compras monetarias."""

    def test_carga_clientes_unicos(
        self, archivos_fixtures: dict[str, Path]
    ) -> None:
        clients = load_clientes(archivos_fixtures["clientes"])

        assert len(clients) == 127
        assert len({client.rut for client in clients}) == len(clients)


    def test_compras_son_montos_no_cantidades(
        self, archivos_fixtures: dict[str, Path]
    ) -> None:
        clients = load_clientes(archivos_fixtures["clientes"])
        con_compras = [client for client in clients if client.compras > 0]

        assert len(con_compras) == 21
        assert sum(client.compras for client in con_compras) > 100_000
        assert max(client.compras for client in con_compras) > 10_000
