
DICT_FORMAT_AND_ENGINE: dict = {
    "xlsx": "openpyxl",
    "xlsb": "pyxlsb",
    "xls": "xlrd"
}

HEADERS_ENG: dict = {
    ("Адм станц назн СНГ", "Адм станц приб СНГ"): "administration_of_the_cis_destination_station",
    ("Адм станц отпр СНГ", "Адм станц отпр СНГ.1", "Адм станц отпр СНГ1"):
        "administration_of_the_cis_departure_station",
    ("Арендатор", "Арендатор вагона по ГВЦ", "Арендатор вагона"): "leaseholder",
    ("Арендатор вагона по внутреннему справочнику",): "wagon_tenant_according_to_internal_directory",
    ("Вагона-км", "Вагоно-км"): "wagon_kilometers",
    ("Вес",): "weight",
    ("Вес тары*порож ваг", "Вес* тары порож ваг"): "tara_weight_of_an_empty_wagon",
    ("Вид перевозки", "Тип транспортировки"): "type_of_transportation",
    ("Вид сообщения",): "type_of_communication_between_countries_by_rail",
    ("Вид контейнера", "Вид специального контейнера", "Вид спецконтейнера", "Тип контейнера"):
        "type_of_special_container",
    ("Вид учета",): "type_of_accounting",
    ("Гос наз", "Государство назначения"): "state_of_destination",
    ("Гос отпр", "Государство отправления"): "state_of_departure",
    ("Гос-во собственник", "Государство собственник вагона"): "state_owner_of_the_wagon",
    ("Груз ОКВЭД",): "cargo_okved",
    ("Груз по ГНГ",): "cargo_according_to_the_harmonized_nomenclature_of_goods_gng",
    ("Грузооборот",): "cargo_turnover",
    ("Грузоотправитель (код)", "Грузоотправитель(код)", "ОКПО грузоотправителя", "Грузоотправитель код ОКПО",
     "Грузоотправитель ОКПО", "Грузоотправитель ОКПО  (код)"): "shipper_okpo",
    ("Грузоотправитель наим по ЕГРПО", "Грузоотправитель по ЕГРПО"): "shipper_according_to_egrpo",
    ("Грузоотправитель наим по ПУЖТ", "Грузоотправитель по ПУЖТ", "Грузоотправитель"): "shipper_by_puzt",
    ("Грузоподъемность", "Грузоподъемность вагона, тн"): "load_capacity",
    ("Грузополучатель (код)", "Грузополучатель код", "Грузополучатель(код)", "ОКПО грузополучателя",
     "Грузополучатель код ОКПО", "Грузополучатель ОКПО"): "consignee_okpo",
    ("Грузополучатель наим по ЕГРПО", "Грузополучатель по ЕГРПО"): "consignee_according_to_egrpo",
    ("Грузополучатель наим по ПУЖТ", "Грузополучатель по ПУЖТ", "Грузополучатель"): "consignee_by_puzt",
    ("Группа груза",): "cargo_group",
    ("Группа груза ОКВЭД", "Группа*груза ОКВЭД"): "cargo_group_okved",
    ("Гр груза по ГО6", "Группа груза по ГО-6", "Группа груза по ГО6"): "group_of_cargo_according_to_go6",
    ("Гр груза по ГО7",): "group_of_cargo_according_to_go7",
    ("Группа груза по ЕТСНГ",): "cargo_group_according_to_etsng",
    ("Гр груза по опер номен", "Гр груза по опер.номен", "Гр груза по опер#номен",
     "Группа груза по оперативной номенклатуре"): "group_of_cargo_according_to_the_operational_nomenclature",
    ("Гр гркза по ЦО22",): "group_of_cargo_according_to_co22",
    ("Гр груза по ЦО21", "Группа груза по ЦО 21"): "group_of_cargo_co21",
    ("Дата отправления",): "departure_date",
    ("дата оформления",): "date_of_registration",
    ("Дата прибытия",): "arrival_date",
    ("Дата раскредитования", "Дата раскредитовки"): "date_of_disbursement",
    ("Дата ЦСУ",): "date_csm",
    ("Дор наз", "Дорога назначения"): "destination_road",
    ("Дорога назн СНГ",): "destination_road_cis",
    ("Дор отпр", "Дорога отправления"): "departure_road",
    ("Дорога отпр СНГ",): "departure_road_cis",
    ("Дор прип ваг", "Дорога приписки вагона"): "wagon_check_in_road",
    ("Дор прип аренд", "Дорога приписки аренды вагона"): "wagon_check_in_road_on_lease",
    ("Категория отпр", "Категория отправки"): "dispatch_category",
    ("Класс груза",): "cargo_class",
    ("Код валюты",): "currency_code",
    ("Код груза", "Код груза  ЕТСНГ", "Код груза ЕТСНГ"): "cargo_code_of_the_etsng",
    ("Код груза ОКВЭД",): "cargo_code_okved",
    ("Код груза по ГНГ",): "cargo_code_according_to_the_harmonized_nomenclature_of_goods_gng",
    ("Код ДЗО",): "subsidiary_and_affiliate_code_of_rzd",
    ("Код искл тарифа", "Код исключительного тарифа (КИТ)"): "exceptional_rate_code",
    ("код подвижного состава",): "rolling_stock_code",
    ("Код ранее перевозимого груза",): "previously_transported_cargo_code",
    ("Код станц назн РФ", "Код станции назначения", "Код станции назначения РФ"): "destination_station_code_of_rf",
    ("Код станц назн СНГ", "Код станции назначения СНГ"): "destination_station_code_of_cis",
    ("Код станц отпр РФ", "Код станции отправления", "Код станции отправления РФ"): "departure_station_code_of_rf",
    ("Код станц отпр СНГ", "Код станции отправления СНГ"):
        "departure_station_code_of_cis",
    ("Код eсловного типа вагона", "Код условного типа вагона", "Код ксловного типа вагона"):
        "code_of_conditional_type_of_wagon",
    ("Кол-во*вагонов", "Количество Вагонов"): "quantity_of_wagons",
    ("Кол-во*контейнеров", "Количество Контейнеров"): "quantity_of_containers",
    ("Месяц", "Месяц отправления"): "departure_month",
    ("Месяц приема груза к перевозке",): "month_of_acceptance_of_cargo_for_transportation",
    ("Месяц раскредитования",): "crediting_month",
    ("Модель вагона",): "wagon_model",
    ("Груз", "Наименование груза"): "name_of_cargo",
    ("Наименование  по ЕТСНГ", "Наименование груза ЕТСНГ"): "name_of_cargo_etsng",
    ("Номер вагона",): "wagon_number",
    ("Номер документа", "Номер ЖД документа"): "document_no",
    ("Номер контейнера",): "container_no",
    ("Объем*перевозок (тн)", "Объем*перевозок(тн)"): "transportation_volume_tons",
    ("Объем перевозок, кг",): "transportation_volume_kg",
    ("Оператор", "Оператор вагона"): "wagon_operator",
    ("Описания типа вагона",): "wagon_type_descriptions",
    ("Отчет сутки отпр", "Отчет. Сутки отпр.", "Отчет# Сутки отпр#"): "departure_day_report",
    ("Отчет сутки приб", "Отчет.сутки приб."): "arrival_day_report",
    ("Паром Усть-Луга-Балтийск", "Паром Цсть-Луга-Балтийск"): "ferry_ust_luga_baltiysk",
    ("плановая дата прибытия",): "planned_arrival_date",
    ("Плательщик", "Плательщик жд тарифа", "Плательщик тарифа"): "payer_of_the_railway_tariff",
    ("Подгрупа груза ОКВЭД", "Подгруппа*груза ОКВЭД"): "cargo_subgroup_okved",
    ("Подгруппа груза",): "sub_group_of_cargo",
    ("Подрод Вагона", "Подрод вагона", "подрод вагона"): "wagon_subgenus",
    ("Подродвагона по ЦО28",): "wagon_subtype_according_to_co28",
    ("Подрод вагона по ЦО29", "Подродвагона по ЦО29"): "wagon_subtype_according_to_co29",
    ("Пояс дальности", "Пояс дальности по Пр 10-01", "Пояс дальности по Пр. 10-01",
     "Пояс дальности по прейскуранту 10-01"): "distance_zone_of_grouping_by_mileage",
    ("Префикс конт", "Префикс контейнера"): "container_prefix",
    ("Признак аренды",): "sign_of_a_lease",
    ("Призн гос отпр", "Признак государства отправления"): "sign_of_the_state_of_departure",
    ("Призн гос назн", "Признак государства назначения"): "sign_of_the_state_of_destination",
    ("Признак доверителя",): "sign_of_the_principal",
    ("Призн иск",): "sign_of_the_exclusive_tariff",
    ("Пр  места расч", "Пр. места расч.", "Пр# места расч#"): "sign_of_the_place_of_settlement",
    ("Пр незач в погр",): "sign_of_non_credited_cargo_at_border_crossings",
    ("Призн погранперех назн РФ", "Признак погранперех назн РФ"):
        "sign_of_the_border_crossing_of_the_destination_of_the_rf",
    ("Призн погранперех назн СНГ", "Признак погранперех назн СНГ"):
        "sign_of_the_border_crossing_of_the_destination_of_the_cis",
    ("Призн погранперех отпр РФ", "Признак погранперех отпр РФ"):
        "sign_of_the_border_crossing_of_the_departure_of_the_rf",
    ("Призн погранперех отпр СНГ", "Признак погранперех отпр СНГ"):
        "sign_of_the_border_crossing_of_the_departure_of_the_cis",
    ("Приз собствен ваг", "Признак собствен ваг", "Признак собственности вагона"): "wagon_ownership_sign",
    ("Призн станц назн РФ",): "sign_of_destination_station_of_the_rf",
    ("Призн станц назн СНГ",): "sign_of_destination_station_of_the_cis",
    ("Призн станц отпр РФ",): "sign_of_departure_station_of_the_rf",
    ("Признак тарифа",): "tariff_sign",
    ("Принадлежность станции назначения СНГ",): "cis_destination_station_affiliation",
    ("Принадлежность станции отправления СНГ",): "cis_departure_station_affiliation",
    ("Провозная плата", "Провозная плата (тариф), без НДС", "Провозная плата/тариф", "Провозная*плата"): "carriage_fee",
    ("Прогнозная дата прибытия",): "estimated_date_of_arrival",
    ("Ранее перевозимый груз",): "previously_transported_cargo",
    ("Регион наз", "Регион назначения"): "destination_region",
    ("Регион отпр", "Регион отправления"): "departure_region",
    ("Род вагона",): "wagon_type",
    ("Род вагона по ЦО29",): "type_of_wagon_according_to_co29",
    ("Собственник вагона по внутреннему справочнику", "Собственник по вн справ", "Собственник по вн.справ",
     "Собственник по вн.справ.", "Собственник по вн#справ"):
        "the_owner_of_the_wagon_according_to_the_internal_directory",
    ("Собственник вагона", "Собственник вагона по ЕГРПО", "Собственник по ЕГРПО"): "wagon_owner_according_to_egrpo",
    ("Станц назн РФ", "Станция назн РФ", "Станция назначения", "Станция назначения РФ"): "rf_destination_station",
    ("Станц назн СНГ", "Станция назначения СНГ"): "cis_destination_station",
    ("Станц отпр РФ", "Станция отпр РФ", "Станция отправления", "Станция отправления РФ"):
        "departure_station_of_the_rf",
    ("Станц отпр СНГ", "Станция отправления СНГ"): "cis_departure_station",
    ("Стан прип аренд", "Станция приписки аренды вагона"): "check_in_station_for_wagon_rental",
    ("Стан прип ваг", "Станция приписки вагона"): "wagon_registration_station",
    ("Субъект назначения РФ", "Субъект федерации наз", "Субъект федерации назначения"): "destination_subject_of_the_rf",
    ("Субъект отправления РФ", "Субъект федерации отп", "Субъект федерации отправления"):
        "subject_of_departure_of_the_rf",
    ("Сумма скидки", "Сумма*скидки"): "discount_amount",
    ("Тарифное расстояние",): "tariff_distance",
    ("Тип парка", "Тип рейса"): "park_type",
    ("Тип сообщения",): "type_of_message",
    ("Тоннажность", "Тоннажность конт", "Тоннажность контейнера", "Футовость контейнера"): "container_tonnage",
    ("Условный тип вагона",): "conditional_wagon_type",
    ("факт дата прибытия",): "actual_date_of_arrival",
    ("Число приема груза к перевозке",): "number_of_cargo_acceptances_for_transportation",
    ("Призн станц отпр СНГ",): "sign_of_the_departure_station_of_the_cis",
    ("ДФЭ",): "teu"
}

DATE_FORMATS: list = [
    "%m/%d/%y",
    "%d.%m.%Y",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y",
    "%d%b%Y"
]

LIST_OF_FLOAT_TYPE: list = [
    "transportation_volume_tons",
    "weight",
    "teu"
]

LIST_OF_INT_TYPE: list = [
    "wagon_kilometers",
    "tara_weight_of_an_empty_wagon",
    "cargo_class",
    "rolling_stock_code",
    "quantity_of_wagons",
    "quantity_of_containers",
    "transportation_volume_kg",
    "cargo_turnover",
    "discount_amount",
    "carriage_fee",
    "tariff_distance",
    "sign_of_the_exclusive_tariff",
    "number_of_cargo_acceptances_for_transportation"
]

LIST_OF_DATE_TYPE: list = [
    "departure_date",
    "arrival_date",
    "date_of_disbursement",
    "departure_day_report",
    "planned_arrival_date",
    "estimated_date_of_arrival",
    "actual_date_of_arrival",
    "arrival_day_report",
    "date_csm"
]

LIST_SPLIT_MONTH: list = [
    "departure_month",
    "crediting_month",
    "month_of_acceptance_of_cargo_for_transportation",
]

LIST_VALUE_NOT_NULL: list = [
    "number_of_cargo_acceptances_for_transportation",
    "transportation_volume_tons",
    "quantity_of_containers",
    "departure_day_report",
    "quantity_of_wagons",
    "wagon_kilometers",
    "cargo_turnover",
    "departure_date"
]

LIST_OF_EMPTY_VALUES: list = [
    "type_of_special_container",
    "dispatch_category"
]
