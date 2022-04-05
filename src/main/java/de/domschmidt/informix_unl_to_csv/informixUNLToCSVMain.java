package de.domschmidt.informix_unl_to_csv;

import de.domschmidt.informix_unl_to_csv.defaults.AutoIncreaseColumnDefault;
import de.domschmidt.informix_unl_to_csv.defaults.ITableDefaultValue;
import de.domschmidt.informix_unl_to_csv.defaults.StaticStringDefault;
import de.domschmidt.informix_unl_to_csv.formatter.DATE_FORMATTER;
import de.domschmidt.informix_unl_to_csv.formatter.ICustomTableColumnFormatter;
import de.domschmidt.informix_unl_to_csv.formatter.MONTH_DAY_TO_DATE_FORMATTER;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class informixUNLToCSVMain {

    private static final Map<String, String> unlFilePerTable = new HashMap<>();
    private static final Pattern databaseNamePattern = Pattern.compile("^\\{ DATABASE ([\\w]+).*$");
    private static final Pattern tablePattern = Pattern.compile("^\\{ TABLE ([\\\"\\w\\.]+).*$");
    private static final Pattern tableUNLPattern = Pattern.compile("^\\{ unload file name = ([\\\"\\w\\.]+).*$");

    // table name, column idx, custom formatter class
    private static final Map<String, Map<String, ICustomTableColumnFormatter>> CUSTOM_TABLE_CONVERTERS;
    // table name, list of strings to be appended
    private static final Map<String, Map<String, ITableDefaultValue>> CUSTOM_TABLE_COLUMN_DEFAULTS;
    // table name, custom schema name
    private static final Map<String, String> CUSTOM_TABLE_SCHEMA;
    // table name, column idx, column name
    private static final Map<String, List<String>> CUSTOM_TABLE_COLUMN_ORDER;

    static {
        final Map<String, Map<String, ICustomTableColumnFormatter>> newCustomConverters = new HashMap<>();

        newCustomConverters.put("termin", Map.of("te_datum", new DATE_FORMATTER()));
        newCustomConverters.put("konzertsaal", Map.of("ks_datum", new DATE_FORMATTER()));
        newCustomConverters.put("istterm", Map.of("it_datum", new DATE_FORMATTER()));
        newCustomConverters.put("menuepunkt", Map.of("mp_erfassdatum", new DATE_FORMATTER()));
        newCustomConverters.put("bn_zugriff_mp", Map.of("bn_mp_erfassdatum", new DATE_FORMATTER()));
        newCustomConverters.put("omvertrag", Map.of(
                "omv_geburtsdatum", new DATE_FORMATTER(),
                "omv_eintrittsdatum", new DATE_FORMATTER(),
                "omv_austrittsdatum", new DATE_FORMATTER(),
                "omv_passgueltig", new DATE_FORMATTER()
        ));
        newCustomConverters.put("vmvertrag", Map.of(
                "vmv_geburtsdatum", new DATE_FORMATTER(),
                "vmv_eintrittsdatum", new DATE_FORMATTER(),
                "vmv_austrittsdatum", new DATE_FORMATTER(),
                "vmv_passgueltig", new DATE_FORMATTER()
        ));
        newCustomConverters.put("anordadressat", Map.of("aoa_faelligtag", new DATE_FORMATTER()));
        newCustomConverters.put("anordaender", Map.of("aag_erstmals", new DATE_FORMATTER()));
        newCustomConverters.put("anordaenderdauer", Map.of("aad_letztmals", new DATE_FORMATTER()));
        newCustomConverters.put("anordanneinzel", Map.of("ane_vzbeginn", new DATE_FORMATTER()));
        newCustomConverters.put("anordannsammel", Map.of("anp_vzbeginn", new DATE_FORMATTER()));
        newCustomConverters.put("anorddauer", Map.of(
                "ada_erstmals", new DATE_FORMATTER(),
                "ada_letztmals", new DATE_FORMATTER()
        ));
        newCustomConverters.put("anordeinzel", Map.of("aei_faelligtag", new DATE_FORMATTER()));
        newCustomConverters.put("ao_betrifft_ta", Map.of(
                "ao_ta_anfangsdatum", new DATE_FORMATTER(),
                "ao_ta_enddatum", new DATE_FORMATTER()
        ));
        newCustomConverters.put("global_prog", Map.of(
                "g_vanfdat", new DATE_FORMATTER(),
                "g_venddat", new DATE_FORMATTER(),
                "g_akt_sz_begin", new DATE_FORMATTER()
        ));
        newCustomConverters.put("anordsammel", Map.of("asa_faelligtag", new DATE_FORMATTER()));
        newCustomConverters.put("haushaltsstelle", Map.of(
                "hst_ansatzdatum", new DATE_FORMATTER(),
                "hst_freigabedatum", new DATE_FORMATTER()
        ));
        newCustomConverters.put("huela", Map.of("hla_datum", new DATE_FORMATTER()));
        newCustomConverters.put("huele", Map.of("hle_datum", new DATE_FORMATTER()));
        newCustomConverters.put("anordnung", Map.of("ao_datum", new DATE_FORMATTER()));
        newCustomConverters.put("ausleihe", Map.of(
                "au_ausgabe", new DATE_FORMATTER(),
                "au_rueckgabe", new DATE_FORMATTER()
        ));
        newCustomConverters.put("bestellung", Map.of(
                "bs_spandatum", new DATE_FORMATTER(),
                "bs_lieferdatum", new DATE_FORMATTER(),
                "bs_bestelldatum", new DATE_FORMATTER(),
                "bs_rueckgabedatum", new DATE_FORMATTER(),
                "bs_zurueckgegeben", new DATE_FORMATTER(),
                "bs_ersteauff", new DATE_FORMATTER(),
                "bs_letzteauff", new DATE_FORMATTER()
        ));
        newCustomConverters.put("konzertvertrag", Map.of("kv_datum", new DATE_FORMATTER()));
        newCustomConverters.put("sdvertrag", Map.of("sdv_datum", new DATE_FORMATTER()));
        newCustomConverters.put("tagegeldsatz", Map.of("ts_gueltigkeit", new DATE_FORMATTER()));
        newCustomConverters.put("sachbezugswert", Map.of("sw_gueltigkeit", new DATE_FORMATTER()));
        newCustomConverters.put("buehne", Map.of(
                "bue_abladedatum", new DATE_FORMATTER(),
                "bue_aufbaudatum", new DATE_FORMATTER(),
                "bue_abbaudatum", new DATE_FORMATTER()
        ));
        newCustomConverters.put("personenreisetage", Map.of(
                "pt_abfahrtstag", new DATE_FORMATTER(),
                "pt_ankunftstag", new DATE_FORMATTER()
        ));
        newCustomConverters.put("reisetage", Map.of(
                "rt_abfahrtstag", new DATE_FORMATTER(),
                "rt_ankunftstag", new DATE_FORMATTER()
        ));
        newCustomConverters.put("benutzer", Map.of("bn_erfassdatum", new DATE_FORMATTER()));
        newCustomConverters.put("tagesdienste", Map.of("td_tag", new DATE_FORMATTER()));
        newCustomConverters.put("ferien", Map.of(
                "fe_beginn", new DATE_FORMATTER(),
                "fe_ende", new DATE_FORMATTER()
        ));
        newCustomConverters.put("db_version", Map.of("dbv_datum", new DATE_FORMATTER()));
        newCustomConverters.put("geschaeftsjahr", Map.of(
                "gj_gueltigkeit", new DATE_FORMATTER(),
                "gj_beginn", new MONTH_DAY_TO_DATE_FORMATTER(),
                "gj_ende", new MONTH_DAY_TO_DATE_FORMATTER()
        ));
        newCustomConverters.put("veranstaltung", Map.of(
                "v_anfangsdatum", new DATE_FORMATTER(),
                "v_enddatum", new DATE_FORMATTER()
        ));
        newCustomConverters.put("v_kostet_ka", Map.of(
                "v_ka_solldatum", new DATE_FORMATTER(),
                "v_ka_istdatum", new DATE_FORMATTER()
        ));

        CUSTOM_TABLE_CONVERTERS = newCustomConverters;

        final Map<String, Map<String, ITableDefaultValue>> newTableColumnDefaults = new HashMap<>();
        newTableColumnDefaults.put("person", Map.of(
                "deleted", new StaticStringDefault("")
        ));
        newTableColumnDefaults.put("v_kostet_ka", Map.of(
                "id", new AutoIncreaseColumnDefault(1)
        ));
        CUSTOM_TABLE_COLUMN_DEFAULTS = newTableColumnDefaults;

        final Map<String, String> customTableSchemas = new HashMap<>();
        customTableSchemas.put("kostenartengruppe", "informix");
        customTableSchemas.put("kostenart", "informix");
        customTableSchemas.put("kostentragergruppe", "informix");
        customTableSchemas.put("kostentraeger", "informix");
        customTableSchemas.put("v_kostet_ka", "informix");
        CUSTOM_TABLE_SCHEMA = customTableSchemas;

        final Map<String, List<String>> customColumnOrders = new HashMap<>();
        customColumnOrders.put("hot_gebucht_v", Arrays.asList(
                "pe_id",
                "v_id"
        ));
        customColumnOrders.put("instrgruppe", Arrays.asList(
                "ig_id",
                "ig_bezeichnung",
                "ig_ordnung"
        ));
        customColumnOrders.put("pe_bei_it", Arrays.asList(
                "pe_id",
                "it_id",
                "pe_it_anwinfo"
        ));
        customColumnOrders.put("ko_komponiert_st", Arrays.asList(
                "ko_id",
                "st_id"
        ));
        customColumnOrders.put("pbu_bei_f", Arrays.asList(
                "pe_id",
                "te_id",
                "pbu_f_bem"
        ));
        customColumnOrders.put("salaergruppe", Arrays.asList(
                "sg_id",
                "sg_bezeichnung"
        ));
        customColumnOrders.put("st_bei_p", Arrays.asList(
                "st_id",
                "te_id"
        ));
        customColumnOrders.put("te_zu_v", Arrays.asList(
                "te_id",
                "v_id"
        ));
        customColumnOrders.put("adresse", Arrays.asList(
                "adr_id",
                "adr_kontaktpers",
                "adr_adrzeile1",
                "adr_adrzeile2",
                "adr_plz",
                "adr_stadt",
                "adr_land",
                "adr_telefon",
                "adr_kurzwahl",
                "adr_fax"
        ));
        customColumnOrders.put("adr_zu_pe", Arrays.asList(
                "adr_id",
                "pe_id",
                "adr_pe_typ"
        ));
        customColumnOrders.put("personenklasse", Arrays.asList(
                "pk_id",
                "pk_kuerzel",
                "pk_bedeutung"
        ));
        customColumnOrders.put("aad_aendert_ada", Arrays.asList(
                "ao_id1",
                "ao_id2"
        ));
        customColumnOrders.put("aae_aendert_ane", Arrays.asList(
                "ao_id1",
                "ao_id2"
        ));
        customColumnOrders.put("aag_aendert_aag", Arrays.asList(
                "ao_id1",
                "ao_id2"
        ));
        customColumnOrders.put("aas_folgt_aas", Arrays.asList(
                "ao_id1",
                "ao_id2"
        ));
        customColumnOrders.put("ann_eintrag_hle", Arrays.asList(
                "ao_id",
                "hle_id"
        ));
        customColumnOrders.put("anordabschlschluss", Arrays.asList(
                "ao_id",
                "aas_typ"
        ));
        customColumnOrders.put("anordadressat", Arrays.asList(
                "aoa_id",
                "aoa_nachname",
                "aoa_adrzeile1",
                "aoa_adrzeile2",
                "aoa_ort",
                "aoa_land",
                "aoa_geldinstitut",
                "aoa_blz",
                "aoa_kontonr",
                "aoa_psdnr",
                "aoa_betragdm",
                "aoa_faelligtag"
        ));
        customColumnOrders.put("anordaender", Arrays.asList(
                "ao_id",
                "aag_erstmals"
        ));
        customColumnOrders.put("anordaenderdauer", Arrays.asList(
                "ao_id",
                "aad_letztmals",
                "aad_faelligzp1",
                "aad_faelligzp2",
                "aad_faelligint"
        ));
        customColumnOrders.put("anordaendereinzel", Arrays.asList(
                "ao_id"
        ));
        customColumnOrders.put("anordanndauer", Arrays.asList(
                "ao_id"
        ));
        customColumnOrders.put("anordanneinzel", Arrays.asList(
                "ao_id",
                "ane_verzugszinsen",
                "ane_vzbeginn",
                "ane_vzdiskont",
                "ane_saeumnis"
        ));
        customColumnOrders.put("anordannsammel", Arrays.asList(
                "ao_id",
                "anp_verzugszinsen",
                "anp_vzbeginn",
                "anp_vzdiskont",
                "anp_saeumnis"
        ));
        customColumnOrders.put("anordannahm", Arrays.asList(
                "ao_id",
                "ann_vermerk"
        ));
        customColumnOrders.put("anordausdauer", Arrays.asList(
                "ao_id"
        ));
        customColumnOrders.put("anordauseinzel", Arrays.asList(
                "ao_id",
                "aue_betragb",
                "aue_betragw"
        ));
        customColumnOrders.put("anordaussammelhaus", Arrays.asList(
                "ao_id"
        ));
        customColumnOrders.put("anordaussammelpers", Arrays.asList(
                "ao_id"
        ));
        customColumnOrders.put("anordauszahl", Arrays.asList(
                "ao_id"
        ));
        customColumnOrders.put("anorddauer", Arrays.asList(
                "ao_id",
                "ada_erstmals",
                "ada_letztmals",
                "ada_faelligzp1",
                "ada_faelligzp2",
                "ada_faelligint",
                "ada_faelligart"
        ));
        customColumnOrders.put("anordeinzel", Arrays.asList(
                "ao_id",
                "aei_faelligtag"
        ));
        customColumnOrders.put("anordsammel", Arrays.asList(
                "ao_id",
                "asa_faelligtag"
        ));
        customColumnOrders.put("anordumbuch", Arrays.asList(
                "ao_id"
        ));
        customColumnOrders.put("ao_betrifft_pe", Arrays.asList(
                "ao_id",
                "pe_id",
                "aoa_id"
        ));
        customColumnOrders.put("ao_betrifft_hst", Arrays.asList(
                "ao_id",
                "hst_id"
        ));
        customColumnOrders.put("ao_betrifft_ta", Arrays.asList(
                "ao_id",
                "ta_id",
                "ao_ta_anfangsdatum",
                "ao_ta_enddatum"
        ));
        customColumnOrders.put("ao_betrifft_tav", Arrays.asList(
                "ao_id",
                "v_id"
        ));
        customColumnOrders.put("ao_setztab_ao", Arrays.asList(
                "ao_id1",
                "ao_id2"
        ));
        customColumnOrders.put("aoa_fuer_ao", Arrays.asList(
                "aoa_id",
                "ao_id"
        ));
        customColumnOrders.put("aum_betrifft_ao", Arrays.asList(
                "ao_id1",
                "ao_id2"
        ));
        customColumnOrders.put("aum_erzeugt_ao", Arrays.asList(
                "ao_id1",
                "ao_id2"
        ));
        customColumnOrders.put("aus_eintrag_hla", Arrays.asList(
                "ao_id",
                "hla_id"
        ));
        customColumnOrders.put("bel_fuer_auh", Arrays.asList(
                "bel_id",
                "ao_id"
        ));
        customColumnOrders.put("bel_zugeordnet_hla", Arrays.asList(
                "bel_id",
                "hla_id"
        ));
        customColumnOrders.put("beleg", Arrays.asList(
                "bel_id",
                "bel_nr",
                "bel_betragdm"
        ));
        customColumnOrders.put("formulare", Arrays.asList(
                "fml_id",
                "fml_kurzname",
                "fml_zeilen",
                "fml_spalten",
                "fml_psdatei"
        ));
        customColumnOrders.put("g_betreffen_sem", Arrays.asList(
                "g_id",
                "sem_id"
        ));
        customColumnOrders.put("global_prog", Arrays.asList(
                "g_id",
                "g_akt_knr",
                "g_vanfdat",
                "g_venddat",
                "g_opt_stack",
                "g_opt_push",
                "g_modus",
                "g_modstr1",
                "g_modstr2",
                "g_modstr3",
                "g_modstr4",
                "g_modstr5",
                "g_modstr6",
                "g_modstr7",
                "g_modstr8",
                "g_mod4",
                "g_akt_id",
                "g_akt_sz_begin",
                "g_akt_hj",
                "g_akt_kap",
                "g_akt_hst",
                "g_in_transaction",
                "g_roll_transaction",
                "g_sys_err",
                "g_new_peid1",
                "g_new_peid2",
                "g_new_peid3",
                "g_new_peid4",
                "g_new_peid5",
                "g_new_peid6",
                "g_new_peid7",
                "g_new_peid8",
                "g_new_peid9",
                "g_new_peid10",
                "g_new_peid11",
                "g_new_peid12",
                "g_new_peid13",
                "g_new_peid14",
                "g_new_peid15",
                "g_new_peid16",
                "g_new_peid17",
                "g_new_peid18",
                "g_new_peid19",
                "g_new_peid20",
                "g_new_peid21",
                "g_new_peid22",
                "g_new_peid23",
                "g_new_peid24",
                "g_new_peid25",
                "g_new_peid26",
                "g_new_peid27",
                "g_new_peid28",
                "g_new_peid29",
                "g_new_peid30",
                "g_new_peid31",
                "g_new_peid32",
                "g_new_peid33",
                "g_new_peid34",
                "g_new_peid35",
                "g_new_peid36",
                "g_new_peid37",
                "g_new_peid38",
                "g_new_peid39",
                "g_new_peid40",
                "g_new_peid41",
                "g_new_peid42",
                "g_new_peid43",
                "g_new_peid44",
                "g_new_peid45",
                "g_new_peid46",
                "g_new_peid47",
                "g_new_peid48",
                "g_new_peid49",
                "g_new_peid50",
                "g_cnt_peid"
        ));
        customColumnOrders.put("haushaltsstelle", Arrays.asList(
                "hst_id",
                "hst_hausjahr",
                "hst_hauskapitel",
                "hst_haustitel",
                "hst_funkziff",
                "hst_typ",
                "hst_ansatzdm",
                "hst_ansatzname",
                "hst_ansatzdatum",
                "hst_ansatzaz",
                "hst_freigabedm",
                "hst_freigabename",
                "hst_freigabedatum",
                "hst_freigabeaz",
                "hst_zweckbest",
                "hst_abgeschlossen"
        ));
        customColumnOrders.put("haushaltstitel", Arrays.asList(
                "hat_id",
                "hat_hauskapitel",
                "hat_haustitel",
                "hat_funkziff",
                "hat_typ",
                "hat_zweckbest"
        ));
        customColumnOrders.put("hat_fuer_hst", Arrays.asList(
                "hat_id",
                "hst_id"
        ));
        customColumnOrders.put("hla_fuer_hst", Arrays.asList(
                "hla_id",
                "hst_id"
        ));
        customColumnOrders.put("hle_fuer_hst", Arrays.asList(
                "hle_id",
                "hst_id"
        ));
        customColumnOrders.put("hst_deckt_hst", Arrays.asList(
                "hst_id1",
                "hst_id2"
        ));
        customColumnOrders.put("huela", Arrays.asList(
                "hla_id",
                "hla_lfdnr",
                "hla_datum",
                "hla_name",
                "hla_betragdm",
                "hla_betragb",
                "hla_betragw",
                "hla_status"
        ));
        customColumnOrders.put("huele", Arrays.asList(
                "hle_id",
                "hle_lfdnr",
                "hle_datum",
                "hle_name",
                "hle_betragdm",
                "hle_status"
        ));
        customColumnOrders.put("semaphore", Arrays.asList(
                "sem_id",
                "sem_name",
                "sem_nummer"
        ));
        customColumnOrders.put("uebersichtsdaten", Arrays.asList(
                "ud_id",
                "ud_typ",
                "ud_haustitel"
        ));
        customColumnOrders.put("sonstzahlung", Arrays.asList(
                "sz_id",
                "sz_kurz",
                "sz_typ",
                "sz_bezeichnung"
        ));
        customColumnOrders.put("archivmat", Arrays.asList(
                "st_id",
                "am_standort",
                "am_signatur",
                "am_inventarnr",
                "am_bemerk",
                "am_besetzungstr",
                "am_besetzungv1",
                "am_besetzungv2",
                "am_besetzungbr",
                "am_besetzungvioc",
                "am_besetzungkb",
                "am_besetzungfl",
                "am_besetzungpfl",
                "am_besetzungobo",
                "am_besetzungeng",
                "am_besetzungklar",
                "am_besetzungbkla",
                "am_besetzungfag",
                "am_besetzungkfag",
                "am_besetzunghor",
                "am_besetzungwtu",
                "am_besetzungtro",
                "am_besetzungpos",
                "am_besetzungtub",
                "am_besetzungpau",
                "am_besetzungschl",
                "am_besetzunghar",
                "am_besetzungklav",
                "am_besetzungcel",
                "am_besetzungcem",
                "am_besetzungsst",
                "am_besetzungsstc",
                "am_dirigierpart",
                "am_klavierauszug"
        ));

        customColumnOrders.put("ausleihe", Arrays.asList(
                "au_id",
                "au_entleiher_pe",
                "au_archivmat_st",
                "au_verleiher_pe",
                "au_ausgabe",
                "au_rueckgabe"
        ));
        customColumnOrders.put("extmusiker", Arrays.asList(
                "pe_id",
                "em_finanzamt_pe",
                "em_agentur_pe",
                "em_arbeitgeber",
                "em_instrument",
                "em_ist_dirigent",
                "em_ist_solist",
                "em_ist_aushverst"
        ));
        customColumnOrders.put("komponist", Arrays.asList(
                "ko_id",
                "ko_vorname",
                "ko_nachname",
                "ko_geburtsjahr",
                "ko_todesjahr",
                "ko_schutzfrist",
                "ko_bem"
        ));
        customColumnOrders.put("omvertrag", Arrays.asList(
                "omv_id",
                "omv_inhaber_pe",
                "omv_finanzamt_pe",
                "omv_agentur_pe",
                "omv_instrgr_ig",
                "omv_personalnr",
                "omv_psdnr",
                "omv_salaergruppe",
                "omv_geburtsdatum",
                "omv_hauptinstr",
                "omv_nebeninstr",
                "omv_zulage",
                "omv_eintrittsdatum",
                "omv_austrittsdatum",
                "omv_passnummer",
                "omv_passgueltig",
                "omv_igrang",
                "omv_als_dirigent",
                "omv_als_solist"
        ));
        customColumnOrders.put("st_bei_a", Arrays.asList(
                "st_id",
                "te_id",
                "st_a_besetzungstr",
                "st_a_besetzungv1",
                "st_a_besetzungv2",
                "st_a_besetzungbr",
                "st_a_besetzungvioc",
                "st_a_besetzungkb",
                "st_a_besetzungfl",
                "st_a_besetzungpfl",
                "st_a_besetzungobo",
                "st_a_besetzungeng",
                "st_a_besetzungklar",
                "st_a_besetzungbkla",
                "st_a_besetzungfag",
                "st_a_besetzungkfag",
                "st_a_besetzunghor",
                "st_a_besetzungwtu",
                "st_a_besetzungtro",
                "st_a_besetzungpos",
                "st_a_besetzungtub",
                "st_a_besetzungpau",
                "st_a_besetzungschl",
                "st_a_besetzunghar",
                "st_a_besetzungklav",
                "st_a_besetzungcel",
                "st_a_besetzungcem",
                "st_a_besetzungsst",
                "st_a_besetzungsstc",
                "st_a_notengebuehr",
                "st_a_rangina"
        ));
        customColumnOrders.put("tgabr_vorkalk", Arrays.asList(
                "v_id",
                "tav_personbetrag",
                "tav_anzpers",
                "tav_anztgabr"
        ));
        customColumnOrders.put("vmvertrag", Arrays.asList(
                "vmv_id",
                "vmv_inhaber_pe",
                "vmv_finanzamt_pe",
                "vmv_personalnr",
                "vmv_psdnr",
                "vmv_salaergruppe",
                "vmv_beruf",
                "vmv_geburtsdatum",
                "vmv_zulage",
                "vmv_eintrittsdatum",
                "vmv_austrittsdatum",
                "vmv_passnummer",
                "vmv_passgueltig"
        ));
        customColumnOrders.put("istterm", Arrays.asList(
                "it_id",
                "it_anr_it",
                "it_abr_it",
                "it_datum",
                "it_uhrzeit",
                "it_dauer",
                "it_art",
                "it_kurztitel",
                "it_frackinfo",
                "it_fzarbeitinfo"
        ));
        customColumnOrders.put("probe", Arrays.asList(
                "te_id",
                "p_auff_te",
                "p_probensaal_ks",
                "p_theaterinfo"
        ));
        customColumnOrders.put("termin", Arrays.asList(
                "te_id",
                "te_tat_it",
                "te_datum",
                "te_tageszeit",
                "te_art",
                "te_kurztitel",
                "te_beginn",
                "te_ende",
                "te_planungsstatus",
                "te_dzuteilung",
                "te_dmenge",
                "te_frackinfo",
                "te_anmerkung",
                "te_notiz"
        ));
        customColumnOrders.put("anordnung", Arrays.asList(
                "ao_id",
                "ao_typ",
                "ao_datum",
                "ao_betragdm",
                "ao_aktenzeichen",
                "ao_gegenstand",
                "ao_begruendung",
                "ao_ds_name",
                "ao_ds_plz",
                "ao_ds_stadt",
                "ao_ds_telefon",
                "ao_ds_nr",
                "ao_ds_zustkasse",
                "ao_anordberech",
                "ao_feststsach",
                "ao_feststrech",
                "ao_status"
        ));
        customColumnOrders.put("voreinstanord", Arrays.asList(
                "vea_id",
                "vea_anordberech",
                "vea_feststsach",
                "vea_feststrech",
                "vea_faelligplus",
                "vea_status",
                "vea_verzugszinsen",
                "vea_saeumnis"
        ));
        customColumnOrders.put("konzertsaal", Arrays.asList(
                "ks_id",
                "ks_ort",
                "ks_datum",
                "ks_name",
                "ks_maxbesetz",
                "ks_fahrtzeit",
                "ks_umkleide",
                "ks_anfahrt",
                "ks_wegzurbue",
                "ks_ansprechpartner",
                "ks_bem"
        ));
        customColumnOrders.put("bestellung", Arrays.asList(
                "bs_id",
                "bs_stueck_st",
                "bs_bestellart",
                "bs_spandatum",
                "bs_lieferdatum",
                "bs_bestelldatum",
                "bs_rueckgabedatum",
                "bs_zurueckgegeben",
                "bs_verlag_id",
                "bs_rechempf_id",
                "bs_versadr_id",
                "bs_versandart",
                "bs_anzahlauff",
                "bs_ersteauff",
                "bs_letzteauff",
                "bs_besetzungstr",
                "bs_besetzungv1",
                "bs_besetzungv2",
                "bs_besetzungbr",
                "bs_besetzungvioc",
                "bs_besetzungkb",
                "bs_besetzungfl",
                "bs_besetzungpfl",
                "bs_besetzungobo",
                "bs_besetzungeng",
                "bs_besetzungklar",
                "bs_besetzungbkla",
                "bs_besetzungfag",
                "bs_besetzungkfag",
                "bs_besetzunghor",
                "bs_besetzungwtu",
                "bs_besetzungtro",
                "bs_besetzungpos",
                "bs_besetzungtub",
                "bs_besetzungpau",
                "bs_besetzungschl",
                "bs_besetzunghar",
                "bs_besetzungklav",
                "bs_besetzungcel",
                "bs_besetzungcem",
                "bs_besetzungsst",
                "bs_besetzungsstc",
                "bs_dirigierpart",
                "bs_klavierauszug"
        ));
        customColumnOrders.put("auffuehrung", Arrays.asList(
                "te_id",
                "a_konzertsaal_ks",
                "a_bezeichnung",
                "a_rdfkanstalt",
                "a_tvanstalt",
                "a_rdfkmitschnitt",
                "a_tvmitschnitt",
                "a_besetzungstr",
                "a_besetzungv1",
                "a_besetzungv2",
                "a_besetzungbr",
                "a_besetzungvioc",
                "a_besetzungkb",
                "a_besetzungfl",
                "a_besetzungpfl",
                "a_besetzungobo",
                "a_besetzungeng",
                "a_besetzungklar",
                "a_besetzungbkla",
                "a_besetzungfag",
                "a_besetzungkfag",
                "a_besetzunghor",
                "a_besetzungwtu",
                "a_besetzungtro",
                "a_besetzungpos",
                "a_besetzungtub",
                "a_besetzungpau",
                "a_besetzungschl",
                "a_besetzunghar",
                "a_besetzungklav",
                "a_besetzungcel",
                "a_besetzungcem",
                "a_besetzungsst",
                "a_besetzungsstc",
                "a_theaterinfo",
                "a_verstbedarfstr",
                "a_verstbedarfv1",
                "a_verstbedarfv2",
                "a_verstbedarfbr",
                "a_verstbedarfvioc",
                "a_verstbedarfkb",
                "a_verstbedarffl",
                "a_verstbedarfpfl",
                "a_verstbedarfobo",
                "a_verstbedarfeng",
                "a_verstbedarfklar",
                "a_verstbedarfbkla",
                "a_verstbedarffag",
                "a_verstbedarfkfag",
                "a_verstbedarfhor",
                "a_verstbedarfwtu",
                "a_verstbedarftro",
                "a_verstbedarfpos",
                "a_verstbedarftub",
                "a_verstbedarfpau",
                "a_verstbedarfschl",
                "a_verstbedarfhar",
                "a_verstbedarfklav",
                "a_verstbedarfcel",
                "a_verstbedarfcem",
                "a_verstbedarfsst",
                "a_verstbedarfsstc",
                "a_notengebuebern"
        ));

        customColumnOrders.put("sem_betrifft_sem", Arrays.asList(
                "sem_id1",
                "sem_id2",
                "sem_sem_bem"
        ));

        customColumnOrders.put("person", Arrays.asList(
                "pe_id",
                "pe_klasse_pk",
                "pe_geschlecht",
                "pe_titel",
                "pe_vorname",
                "pe_nachname",
                "pe_geldinstitut",
                "pe_blz",
                "pe_kontonr",
                "pe_bem",
                "deleted"
        ));
        customColumnOrders.put("jahresgeshon", Arrays.asList(
                "jgh_jahr",
                "jgh_musiker_pe",
                "jgh_typ",
                "jgh_wert"
        ));
        customColumnOrders.put("kv_beinhaltet_te", Arrays.asList(
                "kv_id",
                "te_id",
                "kv_te_phila_ao",
                "kv_te_erstv_ao",
                "kv_te_erstp_ao",
                "kv_te_absphila_ao",
                "kv_te_abserstv_ao",
                "kv_te_abserstp_ao",
                "kv_te_ohonb_soll",
                "kv_te_ohondm_ist",
                "kv_te_solb_ist",
                "kv_te_dirb_ist",
                "kv_te_status"
        ));
        customColumnOrders.put("konzertvertrag", Arrays.asList(
                "kv_id",
                "kv_veranstaltung_v",
                "kv_vertrpart_pe",
                "kv_veranstalter_pe",
                "kv_provausz_ao",
                "kv_provabsetz_ao",
                "kv_auslandinfo",
                "kv_hauptprobe",
                "kv_anspielprobe",
                "kv_notenkinfo",
                "kv_solinfo",
                "kv_dirinfo",
                "kv_waehrung",
                "kv_orchhonb_soll",
                "kv_solb_soll",
                "kv_dirb_soll",
                "kv_provproz_soll",
                "kv_provb_soll",
                "kv_ustinfo_soll",
                "kv_orchhondm_ist",
                "kv_solb_ist",
                "kv_dirb_ist",
                "kv_provproz_ist",
                "kv_provb_ist",
                "kv_ustinfo_ist",
                "kv_provstatus",
                "kv_status",
                "kv_datum"
        ));
        customColumnOrders.put("sdvertrag", Arrays.asList(
                "sdv_id",
                "sdv_musiker_pe",
                "sdv_vertrpart_pe",
                "sdv_typ",
                "sdv_hauptprobe",
                "sdv_anspielprobe",
                "sdv_astinfo",
                "sdv_geshondm_soll",
                "sdv_geshondm_ist",
                "sdv_astbetragdm",
                "sdv_ustbetragdm",
                "sdv_status",
                "sdv_datum"
        ));
        customColumnOrders.put("sdv_beinhaltet_te", Arrays.asList(

                "sdv_id",
                "te_id",
                "sdv_te_aus_ao",
                "sdv_te_fin_ao",
                "sdv_te_absaus_ao",
                "sdv_te_absfin_ao",
                "sdv_te_teildm_soll",
                "sdv_te_teildm_ist",
                "sdv_te_hondm_ist",
                "sdv_te_erstdm_ist",
                "sdv_te_astbetragdm",
                "sdv_te_ustbetragdm",
                "sdv_te_sozbetragdm",
                "sdv_te_vorschussdm",
                "sdv_te_status"
        ));
        customColumnOrders.put("tagegeldsatz", Arrays.asList(
                "ts_id",
                "ts_reisedauer",
                "ts_land",
                "ts_satz",
                "ts_gueltigkeit"
        ));
        customColumnOrders.put("sachbezugswert", Arrays.asList(
                "sw_id",
                "sw_sache",
                "sw_wert",
                "sw_gueltigkeit"
        ));
        customColumnOrders.put("buehne", Arrays.asList(
                "bue_id",
                "bue_zu_te",
                "bue_pause",
                "bue_kontaktperson",
                "bue_avisiert",
                "bue_chor",
                "bue_kinderchor",
                "bue_abladedatum",
                "bue_abladebeginn",
                "bue_abladeende",
                "bue_aufbaudatum",
                "bue_aufbaubeginn",
                "bue_abbaudatum",
                "bue_abbaubeginn",
                "bue_abbauende",
                "bue_hilfskraefte",
                "bue_garderobedir",
                "bue_garderobesol",
                "bue_garderobehr",
                "bue_garderobeda",
                "bue_garderobechor",
                "bue_stuehle",
                "bue_stuehleinfo",
                "bue_notenpulte",
                "bue_notenpulteinfo",
                "bue_basstuehle",
                "bue_basstuehleinfo",
                "bue_dirpodest",
                "bue_dirpodestinfo",
                "bue_bassbretter",
                "bue_bassbrettinfo",
                "bue_cellobretter",
                "bue_cellobrettinfo",
                "bue_podestanzahl",
                "bue_podestflaeche",
                "bue_stufenanzahl",
                "bue_podeststufen",
                "bue_bem"
        ));
        customColumnOrders.put("satz", Arrays.asList(
                "s_id",
                "s_stueck_st",
                "s_reihenfolge",
                "s_bezeichnung"
        ));
        customColumnOrders.put("stueck", Arrays.asList(
                "st_id",
                "st_gattung_gtg",
                "st_volltitel",
                "st_kurztitel",
                "st_edition",
                "st_entstehungsjahr",
                "st_dauer",
                "st_partbesetzstr",
                "st_partbesetzv1",
                "st_partbesetzv2",
                "st_partbesetzbr",
                "st_partbesetzvioc",
                "st_partbesetzkb",
                "st_partbesetzfl",
                "st_partbesetzpfl",
                "st_partbesetzobo",
                "st_partbesetzeng",
                "st_partbesetzklar",
                "st_partbesetzbkla",
                "st_partbesetzfag",
                "st_partbesetzkfag",
                "st_partbesetzhor",
                "st_partbesetzwtu",
                "st_partbesetztro",
                "st_partbesetzpos",
                "st_partbesetztub",
                "st_partbesetzpau",
                "st_partbesetzschl",
                "st_partbesetzhar",
                "st_partbesetzklav",
                "st_partbesetzcel",
                "st_partbesetzcem",
                "st_partbesetzsst",
                "st_partbesetzsstc",
                "st_satznumart"
        ));
        customColumnOrders.put("gattung", Arrays.asList(
                "gtg_id",
                "gtg_nummer",
                "gtg_bezeichnung",
                "gtg_reihenfolge"
        ));
        customColumnOrders.put("pe_erh_ta", Arrays.asList(
                "pe_id",
                "ta_id",
                "pe_ta_vorschuss"
        ));
        customColumnOrders.put("personenreisetage", Arrays.asList(
                "pt_id",
                "pt_person_pe",
                "pt_tgabr_ta",
                "pt_abfahrtstag",
                "pt_abfahrtszeit",
                "pt_ankunftstag",
                "pt_ankunftszeit",
                "pt_land",
                "pt_fruehkuerz",
                "pt_mittagkuerz",
                "pt_abendkuerz",
                "pt_tagegeldanteil"
        ));
        customColumnOrders.put("reisetage", Arrays.asList(
                "rt_id",
                "rt_abrechnung_ta",
                "rt_tagegeldsatz_ts",
                "rt_abfahrtstag",
                "rt_abfahrtszeit",
                "rt_ankunftstag",
                "rt_ankunftszeit",
                "rt_land",
                "rt_fruehkuerz",
                "rt_mittagkuerz",
                "rt_abendkuerz",
                "rt_tagegeldanteil"
        ));
        customColumnOrders.put("sw_fuer_rt", Arrays.asList(
                "sw_id",
                "rt_id"
        ));
        customColumnOrders.put("tgabr", Arrays.asList(
                "ta_id",
                "ta_veranstaltung_v",
                "ta_art",
                "ta_vorschuss",
                "ta_zustand"
        ));
        customColumnOrders.put("menuepunkt", Arrays.asList(
                "mp_id",
                "mp_reihenfolge",
                "mp_tiefe",
                "mp_bezeichnung",
                "mp_funktion",
                "mp_erfassdatum"
        ));
        customColumnOrders.put("benutzer", Arrays.asList(
                "bn_id",
                "bn_kennung",
                "bn_name",
                "bn_erfassdatum"
        ));
        customColumnOrders.put("bn_zugriff_mp", Arrays.asList(
                "bn_id",
                "mp_id",
                "bn_mp_erfassdatum"

        ));
        customColumnOrders.put("dienststelle", Arrays.asList(
                "ds_id",
                "ds_finanzamt_pe",
                "ds_name",
                "ds_adrzeile1",
                "ds_adrzeile2",
                "ds_plz",
                "ds_stadt",
                "ds_telefon",
                "ds_fax",
                "ds_nr",
                "ds_hauskapitel",
                "ds_funkziff",
                "ds_zustkasse",
                "ds_geldinstituth",
                "ds_blzh",
                "ds_kontonrh",
                "ds_geldinstitutn",
                "ds_blzn",
                "ds_kontonrn"
        ));
        customColumnOrders.put("tagesdienste", Arrays.asList(
                "td_id",
                "pe_id",
                "td_tag",
                "td_prdienste",
                "td_auffgdienste",
                "td_rdienste",
                "td_allgdienste",
                "td_frdienste",
                "td_fahrten",
                "td_ausfdienste",
                "td_prspiele",
                "td_vorproben"
        ));
        customColumnOrders.put("fahrt", Arrays.asList(
                "te_id",
                "f_auff_te",
                "f_abfahrtsort",
                "f_ankunftsort",
                "f_busanzahl",
                "f_busgroesse",
                "f_zustiegsort1",
                "f_zustiegsort2",
                "f_zustiegsort3",
                "f_zustiegszeit1",
                "f_zustiegszeit2",
                "f_zustiegszeit3"
        ));
        customColumnOrders.put("ferien", Arrays.asList(

                "fe_id",
                "fe_beginn",
                "fe_bezeichnung",
                "fe_ende"
        ));
        customColumnOrders.put("db_version", Arrays.asList(

                "dbv_id",
                "dbv_version",
                "dbv_datum"
        ));
        customColumnOrders.put("geschaeftsjahr", Arrays.asList(

                "gj_id",
                "gj_gueltigkeit",
                "gj_beginn",
                "gj_ende"
        ));
        customColumnOrders.put("veranstaltung", Arrays.asList(

                "v_id",
                "v_kostentraeger_kt",
                "v_kostenstellennr",
                "v_titel",
                "v_anfangsdatum",
                "v_enddatum",
                "v_entlastverdopp"
        ));
        customColumnOrders.put("ao_zu_v", Arrays.asList(
                "ao_id",
                "v_id",
                "ao_v_haustitel_hst",
                "ao_v_betragdm",
                "ao_v_betragb",
                "ao_v_betragw"
        ));
        customColumnOrders.put("kostenartengruppe", Arrays.asList(
                "kag_id",
                "kag_konto",
                "kag_name"
        ));
        customColumnOrders.put("kostenart", Arrays.asList(
                "ka_id",
                "ka_gruppe_kag",
                "ka_traeger_kt",
                "ka_isdeleted",
                "ka_name"
        ));
        customColumnOrders.put("kostentragergruppe", Arrays.asList(
                "ktg_id",
                "ktg_name"
        ));
        customColumnOrders.put("kostentraeger", Arrays.asList(
                "kt_id",
                "kt_gruppe_ktg",
                "kt_name"
        ));
        customColumnOrders.put("v_kostet_ka", Arrays.asList(
                "id",
                "v_id",
                "ka_id",
                "v_ka_sollwert",
                "v_ka_sollbem",
                "v_ka_solldatum",
                "v_ka_sollwj",
                "v_ka_statsoll",
                "v_ka_istwert",
                "v_ka_istdatum",
                "v_ka_istwj",
                "v_ka_statist"
        ));
        CUSTOM_TABLE_COLUMN_ORDER = customColumnOrders;
    }

    public static void main(final String[] args) {
        final Options options = new Options();

        final Option input = new Option("i", "input", true, "dbimport or dbexport file path");
        final Option output = new Option("o", "output", true, "output path");
        input.setRequired(true);
        output.setRequired(true);
        options.addOption(input);
        options.addOption(output);

        final CommandLineParser parser = new DefaultParser();

        try {
            final CommandLine cmd = parser.parse(options, args);

            final Path inputFilePath = Paths.get(cmd.getOptionValue(input));
            final Path outputFilePath = Paths.get(cmd.getOptionValue(output));
            System.out.println("input file path: " + inputFilePath);
            System.out.println("output file path: " + outputFilePath);

            try (final Stream<String> inputFilePathLineStream = Files.lines(inputFilePath)) {

                final AtomicReference<String> databaseName = new AtomicReference<>();
                final AtomicReference<String> currentTable = new AtomicReference<>();
                final AtomicReference<String> currentTableUNLFile = new AtomicReference<>();
                inputFilePathLineStream.forEachOrdered(line -> {
                    final Matcher matcherDatabaseNamePattern = databaseNamePattern.matcher(line);
                    final Matcher matcherTablePattern = tablePattern.matcher(line);
                    final Matcher matcherTableUNLPattern = tableUNLPattern.matcher(line);

                    if (matcherDatabaseNamePattern.matches()) {
                        databaseName.set(matcherDatabaseNamePattern.group(1));
                    } else if (matcherTablePattern.matches()) {
                        currentTable.set(matcherTablePattern.group(1));
                    } else if (matcherTableUNLPattern.matches()) {
                        currentTableUNLFile.set(matcherTableUNLPattern.group(1));
                    }

                    if (currentTable.get() != null && currentTableUNLFile.get() != null) {
                        unlFilePerTable.put(currentTable.get(), currentTableUNLFile.get());
                        currentTable.set(null);
                        currentTableUNLFile.set(null);
                    }
                });

                System.out.println("database name: " + databaseName.get());
                System.out.println("found " + unlFilePerTable.size() + " tables");

                final Path unlParentPath = inputFilePath.getParent().resolve(databaseName + ".exp");

                final Map<String, String> tableNamesPerCSV = new HashMap<>();

                for (Map.Entry<String, String> currentUnlFilePerTable : unlFilePerTable.entrySet()) {
                    final Path currentUnlPath = unlParentPath.resolve(currentUnlFilePerTable.getValue());
                    String normalizedTableName = currentUnlFilePerTable.getKey().replaceAll("\"", "");

                    final List<String> importOrderedColumnNames = new ArrayList<>();
                    String flatContent = Files.readString(inputFilePath).replaceAll("\n", "");
                    //create table "orveus".pe_bei_it (pe_id integer,it_id integer,pe_it_anwinfo char(1));
                    final String tableCreateString = "create table " + currentUnlFilePerTable.getKey() + " ";
                    flatContent = flatContent.substring(flatContent.indexOf(tableCreateString) + tableCreateString.length());
                    flatContent = flatContent.substring(flatContent.indexOf("(") + 1);
                    flatContent = flatContent.substring(0, flatContent.indexOf(");"));

                    final String[] splittedCols = flatContent.split(", ");
                    for (final String currentCol : splittedCols) {
                        importOrderedColumnNames.add(currentCol.trim().split(" ")[0]);
                    }

                    final String tableNameWithoutSchema = normalizedTableName.substring(normalizedTableName.indexOf(".") + 1);
                    if (CUSTOM_TABLE_SCHEMA.containsKey(tableNameWithoutSchema)) {
                        normalizedTableName = CUSTOM_TABLE_SCHEMA.get(tableNameWithoutSchema) + '.' + tableNameWithoutSchema;
                    }

                    System.out.println("processing " + currentUnlPath + " for table " + normalizedTableName);

                    final String csvName = normalizedTableName + ".csv";
                    final File csvFile = outputFilePath.resolve(csvName).toFile();
                    final BufferedWriter csvWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), StandardCharsets.UTF_16));
                    final Map<String, ICustomTableColumnFormatter> customTableConverters = CUSTOM_TABLE_CONVERTERS.get(tableNameWithoutSchema);
                    final Map<String, ITableDefaultValue> customTableDefaults = CUSTOM_TABLE_COLUMN_DEFAULTS.get(tableNameWithoutSchema);

                    final Reader inputFile = new FileReader(currentUnlPath.toFile().getAbsolutePath(), Charset.forName("IBM850"));
                    final Scanner scanner = new Scanner(inputFile);
                    scanner.useDelimiter(Pattern.compile("\n"));
                    while (scanner.hasNext()) {
                        String line = scanner.next();

                        while (line.endsWith("\\")) {
                            line = line.substring(0, line.length() - 1).concat("\n").concat(scanner.next());
                        }

                        if (line.endsWith("\r")) {
                            line = line.substring(0, line.length() - 1).concat("|");
                        }

                        // backslash escapes the pipe
                        final String[] cells = line.split("(?<!\\\\)\\|", -1);

                        boolean first = true;

                        if (CUSTOM_TABLE_COLUMN_ORDER.containsKey(tableNameWithoutSchema)) {
                            final List<String> orderedColumns = CUSTOM_TABLE_COLUMN_ORDER.get(tableNameWithoutSchema);
                            for (final String orderedColumn : orderedColumns) {
                                final int originalIdx = importOrderedColumnNames.indexOf(orderedColumn);

                                final String currentRowRecord;
                                if (originalIdx < 0) {
                                    currentRowRecord = customTableDefaults.get(orderedColumn).get();
                                } else {
                                    currentRowRecord = cells[originalIdx]
                                            .replaceAll("\"", "\"\"")
                                            .replaceAll("\\\\|", "");
                                }

                                if (!first) {
                                    csvWriter.write(',');
                                }
                                csvWriter.write('\"');

                                if (customTableConverters != null && customTableConverters.containsKey(orderedColumn)) {
                                    csvWriter.write(customTableConverters.get(orderedColumn).convert(
                                            currentRowRecord
                                    ));
                                } else {
                                    csvWriter.write(currentRowRecord);
                                }

                                csvWriter.write('\"');

                                if (first) {
                                    first = false;
                                }
                            }

                        } else {

                            for (int currentRecordColIdx = 0; currentRecordColIdx < cells.length - 1; currentRecordColIdx++) {
                                final String currentRowRecord = cells[currentRecordColIdx]
                                        .replaceAll("\"", "\"\"")
                                        .replaceAll("\\\\|", "");
                                if (!first) {
                                    csvWriter.write(',');
                                }
                                csvWriter.write('\"');

                                if (customTableConverters != null && customTableConverters.containsKey(currentRecordColIdx)) {
                                    csvWriter.write(customTableConverters.get(currentRecordColIdx).convert(
                                            currentRowRecord
                                    ));
                                } else {
                                    csvWriter.write(currentRowRecord);
                                }

                                csvWriter.write('\"');

                                if (first) {
                                    first = false;
                                }
                            }
                        }

                        csvWriter.write("\n");
                    }

                    csvWriter.close();
                    tableNamesPerCSV.put(csvName, normalizedTableName);
                }

                final File importFile = outputFilePath.resolve("import.mssql.sql").toFile();
                final BufferedWriter importFileWriter = new BufferedWriter(new FileWriter(importFile, StandardCharsets.UTF_8));

                for (final Map.Entry<String, String> tableNamePerCSV : tableNamesPerCSV.entrySet()) {
                    importFileWriter.write("BULK INSERT " + tableNamePerCSV.getValue()
                            + "\n FROM '/var/opt/mssql/backups/init/" + tableNamePerCSV.getKey() + "'"
                            + "\n WITH (FORMAT = \"CSV\", ROWTERMINATOR = \"\\n\", KEEPIDENTITY);\n\n");
                }

                importFileWriter.write(
                        "EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';\n" +
                                "\n" +
                                "SET IDENTITY_INSERT orveus.person ON;\n" +
                                "\n" +
                                "INSERT INTO orveus.person (pe_id, pe_klasse_pk, pe_geschlecht, pe_titel, pe_vorname, pe_nachname, pe_geldinstitut,\n" +
                                "                           pe_blz, pe_kontonr, pe_bem)\n" +
                                "SELECT distinct a.pe_id,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                'GELÖSCHTE PERSON',\n" +
                                "                'GELÖSCHTE PERSON',\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL\n" +
                                "FROM orveus.ao_betrifft_pe a\n" +
                                "where pe_id not in (SELECT b.pe_id from orveus.person b where a.pe_id = b.pe_id);\n" +
                                "\n" +
                                "INSERT INTO orveus.person (pe_id, pe_klasse_pk, pe_geschlecht, pe_titel, pe_vorname, pe_nachname, pe_geldinstitut,\n" +
                                "                           pe_blz, pe_kontonr, pe_bem)\n" +
                                "SELECT distinct a.pe_id,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                'GELÖSCHTE PERSON',\n" +
                                "                'GELÖSCHTE PERSON',\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL\n" +
                                "FROM orveus.adr_zu_pe a\n" +
                                "where pe_id not in (SELECT b.pe_id from orveus.person b where a.pe_id = b.pe_id);\n" +
                                "\n" +
                                "INSERT INTO orveus.person (pe_id, pe_klasse_pk, pe_geschlecht, pe_titel, pe_vorname, pe_nachname, pe_geldinstitut,\n" +
                                "                           pe_blz, pe_kontonr, pe_bem)\n" +
                                "SELECT distinct a.omv_finanzamt_pe,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                'GELÖSCHTE PERSON',\n" +
                                "                'GELÖSCHTE PERSON',\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL\n" +
                                "FROM orveus.omvertrag a\n" +
                                "where omv_finanzamt_pe not in (SELECT b.pe_id from orveus.person b where a.omv_finanzamt_pe = b.pe_id)\n" +
                                "  and omv_finanzamt_pe IS NOT NULL;\n" +
                                "\n" +
                                "INSERT INTO orveus.person (pe_id, pe_klasse_pk, pe_geschlecht, pe_titel, pe_vorname, pe_nachname, pe_geldinstitut,\n" +
                                "                           pe_blz, pe_kontonr, pe_bem)\n" +
                                "SELECT distinct a.ds_finanzamt_pe,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                'GELÖSCHTE PERSON',\n" +
                                "                'GELÖSCHTE PERSON',\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL\n" +
                                "FROM orveus.dienststelle a\n" +
                                "where ds_finanzamt_pe not in (SELECT b.pe_id from orveus.person b where a.ds_finanzamt_pe = b.pe_id)\n" +
                                "  and ds_finanzamt_pe IS NOT NULL;\n" +
                                "\n" +
                                "INSERT INTO orveus.person (pe_id, pe_klasse_pk, pe_geschlecht, pe_titel, pe_vorname, pe_nachname, pe_geldinstitut,\n" +
                                "                           pe_blz, pe_kontonr, pe_bem)\n" +
                                "SELECT distinct a.vmv_finanzamt_pe,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                'GELÖSCHTE PERSON',\n" +
                                "                'GELÖSCHTE PERSON',\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL,\n" +
                                "                NULL\n" +
                                "FROM orveus.vmvertrag a\n" +
                                "where vmv_finanzamt_pe not in (SELECT b.pe_id from orveus.person b where a.vmv_finanzamt_pe = b.pe_id)\n" +
                                "  and vmv_finanzamt_pe IS NOT NULL;\n" +
                                "\n" +
                                "SET IDENTITY_INSERT orveus.person OFF;\n" +
                                "\n" +
                                "SET IDENTITY_INSERT orveus.anordadressat ON;\n" +
                                "\n" +
                                "INSERT INTO orveus.anordadressat (aoa_id, aoa_nachname)\n" +
                                "SELECT a.aoa_id, 'Adresse ist älter als 10 Jahre'\n" +
                                "FROM orveus.aoa_fuer_ao a\n" +
                                "where a.aoa_id not in (SELECT b.aoa_id from orveus.anordadressat b where a.aoa_id = b.aoa_id);\n" +
                                "\n" +
                                "SET IDENTITY_INSERT orveus.anordadressat OFF;\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.ao_betrifft_pe\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.ao_betrifft_hst\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.ao_zu_v\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.ao_setztab_ao\n" +
                                "where ao_id1 not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id1\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.ao_setztab_ao\n" +
                                "where ao_id2 not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id2\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.aoa_fuer_ao\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.aad_aendert_ada\n" +
                                "where ao_id1 not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id1\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.aae_aendert_ane\n" +
                                "where ao_id1 not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id1\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.ann_eintrag_hle\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordanneinzel\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordannahm\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordeinzel\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordaender\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordaenderdauer\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordannsammel\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordauseinzel\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordaussammelhaus\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordaussammelpers\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordauszahl\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anorddauer\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.anordsammel\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.aus_eintrag_hla\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.bel_fuer_auh\n" +
                                "where ao_id not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.extmusiker\n" +
                                "where em_agentur_pe not in (\n" +
                                "    select b.pe_id\n" +
                                "    from orveus.person b\n" +
                                "    where b.pe_id = em_agentur_pe\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.extmusiker\n" +
                                "where em_finanzamt_pe not in (\n" +
                                "    select b.pe_id\n" +
                                "    from orveus.person b\n" +
                                "    where b.pe_id = em_finanzamt_pe\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.extmusiker\n" +
                                "where pe_id not in (\n" +
                                "    select b.pe_id\n" +
                                "    from orveus.person b\n" +
                                "    where b.pe_id = pe_id\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.hst_deckt_hst\n" +
                                "where hst_id2 not in (\n" +
                                "    select b.hst_id\n" +
                                "    from orveus.haushaltsstelle b\n" +
                                "    where b.hst_id = hst_id2\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.hst_deckt_hst\n" +
                                "where hst_id1 not in (\n" +
                                "    select b.hst_id\n" +
                                "    from orveus.haushaltsstelle b\n" +
                                "    where b.hst_id = hst_id1\n" +
                                ");\n" +
                                "\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.aag_aendert_aag\n" +
                                "where ao_id1 not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id1\n" +
                                ");\n" +
                                "\n" +
                                "delete\n" +
                                "from orveus.aas_folgt_aas\n" +
                                "where ao_id1 not in (\n" +
                                "    select b.ao_id\n" +
                                "    from orveus.anordnung b\n" +
                                "    where b.ao_id = ao_id1\n" +
                                ");\n" +
                                "\n" +
                                "UPDATE orveus.person\n" +
                                "set DELETED = 1\n" +
                                "where pe_vorname = 'GELÖSCHTE PERSON         ';\n" +
                                "\n" +
                                "DELETE\n" +
                                "FROM orveus.personenklasse\n" +
                                "where pk_kuerzel is null\n" +
                                "   or pk_bedeutung is null;\n" +
                                "\n" +
                                "INSERT INTO orveus.personenklasse (pk_bedeutung, pk_kuerzel)\n" +
                                "VALUES ('GELÖSCHTE PERSONENKLASSE', 'DEL');\n" +
                                "\n" +
                                "UPDATE orveus.person\n" +
                                "SET pe_klasse_pk = (SELECT pk_id from orveus.personenklasse where pk_kuerzel = 'DEL')\n" +
                                "where pe_klasse_pk NOT IN (SELECT pk_id from orveus.personenklasse);\n" +
                                "\n" +
                                "INSERT INTO [orveus].[user] (username, id, recorded, updated, deleted, password, user_details_id)\n" +
                                "SELECT adistinct.hle_name, (NEXT VALUE FOR orveus.orveus_seq), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, '', NULL\n" +
                                "FROM (SELECT DISTINCT a.hle_name\n" +
                                "      from orveus.huele a\n" +
                                "      where a.hle_name not in (select b.username from [orveus].[user] b where b.username = a.hle_name)) adistinct;\n" +
                                "\n" +
                                "INSERT INTO orveus.[user] (username, id, recorded, updated, deleted, password, user_details_id)\n" +
                                "SELECT adistinct.hla_name, (NEXT VALUE FOR orveus.orveus_seq), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, '', NULL\n" +
                                "FROM (SELECT DISTINCT a.hla_name\n" +
                                "      from orveus.huela a\n" +
                                "      where a.hla_name not in (select b.username from [orveus].[user] b where b.username = a.hla_name)) adistinct;\n" +
                                "\n" +
                                "\n" +
                                "insert into orveus.user_details (firstname, lastname, id, user_id, recorded, updated)\n" +
                                "select '',\n" +
                                "       '',\n" +
                                "       (NEXT VALUE FOR orveus.orveus_seq),\n" +
                                "       usr.id,\n" +
                                "       CURRENT_TIMESTAMP,\n" +
                                "       CURRENT_TIMESTAMP\n" +
                                "from [orveus].[user] usr\n" +
                                "where not exists(select ex_user.id from orveus.user_details ex_user where ex_user.user_id = usr.id);\n" +
                                "\n" +
                                "\n" +
                                "update USR\n" +
                                "set user_details_id = (SELECT det.id from orveus.user_details det where det.user_id = USR.id)\n" +
                                "from [orveus].[user] USR\n" +
                                "where user_details_id is null;\n" +
                                "\n" +
                                "EXEC sp_msforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL';\n"
                );

                importFileWriter.close();
            } catch (final IOException e) {
                System.out.println(e.getMessage());
                System.exit(2);
            }
        } catch (final ParseException pe) {
            System.out.println(pe.getMessage());
            System.exit(1);
        }
    }

}
