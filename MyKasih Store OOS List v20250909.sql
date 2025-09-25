with
item as (
select my.mykasih_productcategory, my.mykasih_name, my.mykasih_barcode
, goo.goo_no, goo.goo_na, goo.plu_no
, goo.osunt_na
, goo.dps_no
from source_current.mykasih_list my
join source_current.mkfgoomi goo on goo.plu_no = my.barcode
) 
, store as (
select str.str_no, str.state, com.code_txt state_name 
from source_current.mkfstrmi str 
left join source_current.mkfcommi com on str.state = com.code_no and com.comm_no = 'BS_STATE'
)
, stomi as (
    select str_no, goo_no, sto_date, qty
    from (
        select str_no, goo_no, sto_date, qty,
               row_number() over(partition by str_no, goo_no, sto_date order by extracted_date desc) rn
        from source_current.mkfstomi
    ) s
    where rn = 1
)
, pnc as (select * from source_current.mkfgntmi)
, STO_CTE AS (
    select str_no,sto_date,goo_no,qty 
    from ( select str_no,sto_Date,goo_no,qty 
        from stomi
        union all 
        select s.str_no,s.sto_Date,nvl(gn.goo_no2,s.goo_no) goo_no,(s.qty* nvl(gn.qty2,1)/nvl(gn.qty1,1)) qty
        from stomi s
          left join pnc gn on s.goo_no = gn.goo_no1
    )
)
, main as (
select distinct goo.mykasih_productcategory, goo.mykasih_name, goo.mykasih_barcode
, goo.goo_no as item_code, goo.goo_na as item_name, goo.plu_no as barcode
, goo.osunt_na as uom
, goo.dps_no as class_code, cla.dps_na as class_name
, sli.dps_no as subline_code, sli.dps_na as subline_name
, lin.dps_no as line_code, lin.dps_na as line_name
, gos.str_no store_code
, str.state, str.state_name
, CASE 
    WHEN COALESCE(gos.stop_pur, 'N') = 'N' THEN 'Yes'
    WHEN COALESCE(gos.stop_pur, 'N') = 'Y' THEN 'No'
    END as orderable
, CASE 
    WHEN COALESCE(gos.stop_sal, 'N') = 'N' THEN 'Yes'
    WHEN COALESCE(gos.stop_sal, 'N') = 'Y' THEN 'No'
    END as sellable
, CASE 
    WHEN COALESCE(gos.stop_rji, 'N') = 'N' THEN 'Yes'
    WHEN COALESCE(gos.stop_rji, 'N') = 'Y' THEN 'No'
    END as returnable
, CASE 
    WHEN COALESCE(gos.dc_ret_yn, 'Y') = 'Y' THEN 'Yes'
    WHEN COALESCE(gos.dc_ret_yn, 'Y') = 'N' THEN 'No'
    END as dc_returnable
, CASE 
    WHEN COALESCE(gos.goo_status, '1') <> '9' THEN 'Active'
    WHEN COALESCE(gos.goo_status, '1') = '9' THEN 'Not Active'
    END as item_status
, gos.sup_no as supplier_code
, sup.sup_na as supplier_name
, case 
    when gos.ord_send = '0' then 'DC'
    when gos.ord_send = '3' then 'DD'
    else gos.ord_Send end as str_item_distribution_type
, gos.dstr_no as str_item_distribution_warehouse
, gos.iprice as str_item_cost_price, gos.sprice as str_item_sell_price
, sto.sto_date as stock_date, sto.qty::numeric(10,2) as stock_qty
from item goo 
join source_current.mkfdpsmi cla on substring(goo.dps_no,1,8) = cla.dps_no
join source_current.mkfdpsmi sli on substring(goo.dps_no,1,6) = sli.dps_no
join source_current.mkfdpsmi lin on substring(goo.dps_no,1,4) = lin.dps_no
join source_current.mkfgosmi_kk gos on goo.goo_no = gos.goo_no
join store str on gos.str_no = str.str_no

join source_current.mkfsupmi sup on gos.sup_no = sup.sup_no
join STO_CTE sto on gos.str_no = sto.str_no and gos.goo_no = sto.goo_no
where sto.qty <= 0
)
select * from main