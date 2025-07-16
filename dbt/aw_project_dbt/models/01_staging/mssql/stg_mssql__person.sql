with
    person as (
        select
            cast(BusinessEntityID as int) as pk_person
            , cast(PersonType as varchar) as person_type
            , cast(NameStyle as boolean) as
            , cast(Title as varchar) as title_name
            , cast(FirstName as varchar) as first_name
            , cast(MiddleName as varchar) as middle_name
            , cast(LastName as varchar) as last_name
            , cast(Suffix as varchar) as suffix
            , cast(EmailPromotion as int) as email_promotion
            , cast(AdditionalContactInfo as varchar) as additional_contact_info
            , cast(Demographics as varchar) as demographics
            , cast(rowguid as varchar) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_person_person' ) }}     
    )

select *
from person
