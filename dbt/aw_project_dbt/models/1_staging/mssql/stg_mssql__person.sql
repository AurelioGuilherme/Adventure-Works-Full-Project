with
    person as (
        select
            cast(BusinessEntityID as int) as pk_person
            , cast(PersonType as string) as person_type
            , cast(NameStyle as boolean) as
            , cast(Title as string) as title_name
            , cast(FirstName as string) as first_name
            , cast(MiddleName as string) as middle_name
            , cast(LastName as string) as last_name
            , cast(Suffix as string) as suffix
            , cast(EmailPromotion as int) as email_promotion
            , cast(AdditionalContactInfo as string) as additional_contact_info
            , cast(Demographics as string) as demographics
            , cast(rowguid as string) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_person_person' ) }}     
    )

select *
from person
