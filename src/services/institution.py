from dataclasses import dataclass

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.institution import Department, Institution
from src.schemas.institution import (
    DepartmentListResponse,
    DepartmentResponse,
    InstitutionListResponse,
    InstitutionResponse,
)


@dataclass(frozen=True)
class InstitutionSeed:
    code: str
    name: str


@dataclass(frozen=True)
class DepartmentSeed:
    institution_code: str
    name: str


NULP_INSTITUTIONS: tuple[InstitutionSeed, ...] = (
    InstitutionSeed("ІАДУ", "Адміністрування, державного управління та професійного розвитку"),
    InstitutionSeed("ІАРД", "Архітектура та дизайн"),
    InstitutionSeed("ІБІБ", "Будівництво, інфраструктура та безпека життєдіяльності"),
    InstitutionSeed("ІГДГ", "Геодезія"),
    InstitutionSeed("ІГСН", "Гуманітарні та соціальні науки"),
    InstitutionSeed("ІНЕМ", "Економіка і менеджмент"),
    InstitutionSeed("ІЕСК", "Енергетика та системи керування"),
    InstitutionSeed("ІКТЕ", "Інформаційно-комунікаційних технологій та електронної інженерії"),
    InstitutionSeed("ІКНІ", "Комп'ютерні науки та інформаційні технології"),
    InstitutionSeed("ІКТА", "Комп'ютерні технології, автоматика та метрологія"),
    InstitutionSeed("ІМІТ", "Механічна інженерія та транспорт"),
    InstitutionSeed("ІПМТ", "Поліграфії та медійних технологій"),
    InstitutionSeed("ІППТ", "Просторове планування та перспективні технології"),
    InstitutionSeed("ІППО", "Право, психологія та інноваційна освіта"),
    InstitutionSeed("ІМФН", "Прикладна математика та фундаментальні науки"),
    InstitutionSeed("ІСТР", "Сталий розвиток"),
    InstitutionSeed("ІХХТ", "Хімія та хімічні технології"),
    InstitutionSeed("МІОК", "Освіта, культура та зв'язки з діаспорою"),
)

NULP_DEPARTMENTS: tuple[DepartmentSeed, ...] = (
    DepartmentSeed("ІАДУ", "Кафедра адміністративного та фінансового менеджменту"),
    DepartmentSeed("ІАДУ", "Кафедра публічного врядування"),
    DepartmentSeed("ІАДУ", "Кафедра регіонального та місцевого розвитку"),
    DepartmentSeed("ІАДУ", "Кафедра теоретичної та прикладної економіки"),
    DepartmentSeed("ІАДУ", "Кафедра управління проектами"),
    DepartmentSeed("ІАРД", "Кафедра архітектури та реставрації"),
    DepartmentSeed("ІАРД", "Кафедра архітектурного проектування"),
    DepartmentSeed("ІАРД", "Кафедра архітектурного проектування та інженерії"),
    DepartmentSeed("ІАРД", "Кафедра візуального дизайну і мистецтва"),
    DepartmentSeed("ІАРД", "Кафедра дизайну архітектурного середовища"),
    DepartmentSeed("ІАРД", "Кафедра дизайну та основ архітектури"),
    DepartmentSeed("ІАРД", "Кафедра містобудування"),
    DepartmentSeed("ІБІБ", "Кафедра автомобільних доріг та мостів"),
    DepartmentSeed("ІБІБ", "Кафедра будівельних конструкцій та мостів"),
    DepartmentSeed("ІБІБ", "Кафедра будівельного виробництва"),
    DepartmentSeed("ІБІБ", "Кафедра гідротехніки та водної інженерії"),
    DepartmentSeed("ІБІБ", "Кафедра загальновійськової підготовки"),
    DepartmentSeed("ІБІБ", "Кафедра опору матеріалів та будівельної механіки"),
    DepartmentSeed("ІБІБ", "Кафедра теплогазопостачання і вентиляції"),
    DepartmentSeed("ІГДГ", "Кафедра вищої геодезії та астрономії"),
    DepartmentSeed("ІГДГ", "Кафедра геодезії"),
    DepartmentSeed("ІГДГ", "Кафедра інженерної геодезії"),
    DepartmentSeed("ІГДГ", "Кафедра кадастру територій"),
    DepartmentSeed("ІГДГ", "Кафедра картографії та геопросторового моделювання"),
    DepartmentSeed("ІГДГ", "Кафедра фотограмметрії та геоінформатики"),
    DepartmentSeed("ІГСН", "Кафедра іноземних мов гуманітарно-соціального спрямування"),
    DepartmentSeed("ІГСН", "Кафедра іноземних мов технічного спрямування"),
    DepartmentSeed("ІГСН", "Кафедра історії, музеєзнавства та культурної спадщини"),
    DepartmentSeed("ІГСН", "Кафедра політології та міжнародних відносин"),
    DepartmentSeed("ІГСН", "Кафедра соціальних комунікацій та інформаційної діяльності"),
    DepartmentSeed("ІГСН", "Кафедра соціології та соціальної роботи"),
    DepartmentSeed("ІГСН", "Кафедра української мови"),
    DepartmentSeed("ІГСН", "Кафедра фізичного виховання"),
    DepartmentSeed("ІГСН", "Кафедра філософії"),
    DepartmentSeed("ІНЕМ", "Кафедра економіки підприємства та інвестицій"),
    DepartmentSeed("ІНЕМ", "Кафедра зовнішньоекономічної та митної діяльності"),
    DepartmentSeed("ІНЕМ", "Кафедра маркетингу і логістики"),
    DepartmentSeed("ІНЕМ", "Кафедра менеджменту і міжнародного підприємництва"),
    DepartmentSeed("ІНЕМ", "Кафедра менеджменту організацій"),
    DepartmentSeed("ІНЕМ", "Кафедра менеджменту персоналу та адміністрування"),
    DepartmentSeed("ІНЕМ", "Кафедра обліку та аналізу"),
    DepartmentSeed("ІНЕМ", "Кафедра фінансів"),
    DepartmentSeed("ІЕСК", "Кафедра автоматизації та комп'ютерно-інтегрованих технологій"),
    DepartmentSeed("ІЕСК", "Кафедра електроенергетичних систем"),
    DepartmentSeed("ІЕСК", "Кафедра електромеханічних та електротехнічних систем"),
    DepartmentSeed("ІЕСК", "Кафедра теоретичної та загальної електротехніки"),
    DepartmentSeed("ІЕСК", "Кафедра теплоенергетики, теплових та атомних електричних станцій"),
    DepartmentSeed("ІКТЕ", "Кафедра електронних засобів інформаційно-комп’ютерних технологій"),
    DepartmentSeed("ІКТЕ", "Кафедра електронної інженерії"),
    DepartmentSeed("ІКТЕ", "Кафедра інформаційно-комунікаційних технологій"),
    DepartmentSeed("ІКТЕ", "Кафедра напівпровідникової електроніки"),
    DepartmentSeed("ІКТЕ", "Кафедра програмно-апаратних систем інфокомунікацій"),
    DepartmentSeed("ІКТЕ", "Кафедра радіоелектронних технологій інформаційних систем"),
    DepartmentSeed("ІКНІ", "Кафедра автоматизованих систем управління"),
    DepartmentSeed("ІКНІ", "Кафедра інформаційних систем та мереж"),
    DepartmentSeed("ІКНІ", "Кафедра інформаційних технологій видавничої справи"),
    DepartmentSeed("ІКНІ", "Кафедра прикладної лінгвістики"),
    DepartmentSeed("ІКНІ", "Кафедра програмного забезпечення"),
    DepartmentSeed("ІКНІ", "Кафедра систем автоматизованого проектування"),
    DepartmentSeed("ІКНІ", "Кафедра систем віртуальної реальності"),
    DepartmentSeed("ІКНІ", "Кафедра систем штучного інтелекту"),
    DepartmentSeed("ІКТА", "Кафедра безпеки інформаційних технологій"),
    DepartmentSeed("ІКТА", "Кафедра електронних обчислювальних машин"),
    DepartmentSeed("ІКТА", "Кафедра захисту інформації"),
    DepartmentSeed("ІКТА", "Кафедра інтелектуальної мехатроніки і роботики"),
    DepartmentSeed("ІКТА", "Кафедра інформаційно-вимірювальних технологій"),
    DepartmentSeed("ІКТА", "Кафедра комп'ютеризованих систем автоматики"),
    DepartmentSeed("ІКТА", "Кафедра спеціалізованих комп'ютерних систем"),
    DepartmentSeed("ІМІТ", "Кафедра авіаційної та виробничої інженерії"),
    DepartmentSeed("ІМІТ", "Кафедра автомобільного транспорту"),
    DepartmentSeed("ІМІТ", "Кафедра залізничного транспорту"),
    DepartmentSeed("ІМІТ", "Кафедра матеріалознавства та інженерії матеріалів"),
    DepartmentSeed("ІМІТ", "Кафедра проєктування машин та автомобільного інжинірингу"),
    DepartmentSeed("ІМІТ", "Кафедра робототехніки та інтегрованих технологій машинобудування"),
    DepartmentSeed("ІМІТ", "Кафедра технічної механіки та інженерної графіки"),
    DepartmentSeed("ІМІТ", "Кафедра транспортних технологій"),
    DepartmentSeed("ІПМТ", "Кафедра графіки та мистецтва книги"),
    DepartmentSeed("ІПМТ", "Кафедра комп'ютеризованих комплексів поліграфічних та пакувальних виробництв"),
    DepartmentSeed("ІПМТ", "Кафедра комп'ютерних технологій у видавничо-поліграфічних процесах"),
    DepartmentSeed("ІПМТ", "Кафедра медійних технологій, інформаційної та книжкової справи"),
    DepartmentSeed("ІПМТ", "Кафедра менеджменту та маркетингу у видавничо-поліграфічній справі"),
    DepartmentSeed("ІПМТ", "Кафедра мультимедійних технологій"),
    DepartmentSeed("ІПМТ", "Кафедра поліграфічних технологій та паковань"),
    DepartmentSeed("ІППО", "Кафедра адміністративного та інформаційного права"),
    DepartmentSeed("ІППО", "Кафедра журналістики та засобів масової комунікації"),
    DepartmentSeed("ІППО", "Кафедра міжнародного та кримінального права"),
    DepartmentSeed("ІППО", "Кафедра педагогіки та інноваційної освіти"),
    DepartmentSeed("ІППО", "Кафедра практичної психології ментального здоров'я"),
    DepartmentSeed("ІППО", "Кафедра теоретичної та прикладної психології"),
    DepartmentSeed("ІППО", "Кафедра теорії пpaвa та конституціоналізму"),
    DepartmentSeed("ІППО", "Кафедра цивільного права та процесу"),
    DepartmentSeed("ІМФН", "Кафедра вищої математики"),
    DepartmentSeed("ІМФН", "Кафедра загальної фізики"),
    DepartmentSeed("ІМФН", "Кафедра міжнародної інформації"),
    DepartmentSeed("ІМФН", "Кафедра обчислювальної математики та програмування"),
    DepartmentSeed("ІМФН", "Кафедра прикладної математики"),
    DepartmentSeed("ІМФН", "Кафедра прикладної фізики і наноматеріалознавства"),
    DepartmentSeed("ІППТ", "Кафедра економіки і маркетингу"),
    DepartmentSeed("ІППТ", "Кафедра інформаційних систем і технологій"),
    DepartmentSeed("ІППТ", "Кафедра фінансів, обліку і аналізу"),
    DepartmentSeed("ІСТР", "Кафедра безпеки праці і життєдіяльності людини"),
    DepartmentSeed("ІСТР", "Кафедра екології та збалансованого природокористування"),
    DepartmentSeed("ІСТР", "Кафедра екологічної безпеки та природоохоронної діяльності"),
    DepartmentSeed("ІСТР", "Кафедра підприємництва та екологічної експертизи товарів"),
    DepartmentSeed("ІСТР", "Кафедра туризму"),
    DepartmentSeed("ІХХТ", "Кафедра органічної хімії"),
    DepartmentSeed("ІХХТ", "Кафедра технології біологічно активних сполук, фармації та біотехнології"),
    DepartmentSeed("ІХХТ", "Кафедра технології органічних продуктів"),
    DepartmentSeed("ІХХТ", "Кафедра фізичної, аналітичної та загальної хімії"),
    DepartmentSeed("ІХХТ", "Кафедра хімії і технології неорганічних речовин"),
    DepartmentSeed("ІХХТ", "Кафедра хімічної інженерії"),
    DepartmentSeed("ІХХТ", "Кафедра хімічної технології переробки нафти та газу"),
    DepartmentSeed("ІХХТ", "Кафедра хімічної технології переробки пластмас"),
    DepartmentSeed("ІХХТ", "Кафедра хімічної технології силікатів"),
)


class InstitutionService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_institutions(self) -> InstitutionListResponse:
        total = await self.session.scalar(select(func.count()).select_from(Institution))
        result = await self.session.execute(
            select(Institution)
            .where(Institution.is_active.is_(True))
            .order_by(Institution.sort_order.asc())
        )
        institutions = result.scalars().all()
        return InstitutionListResponse(
            items=[InstitutionResponse.model_validate(institution) for institution in institutions],
            total=total or 0,
        )

    async def list_departments(
        self,
        *,
        institution_code: str | None = None,
        search: str | None = None,
    ) -> DepartmentListResponse:
        filters = [Department.is_active.is_(True), Institution.is_active.is_(True)]
        if institution_code:
            filters.append(Institution.code == institution_code)
        if search:
            pattern = f"%{search.strip()}%"
            filters.append(or_(Department.name.ilike(pattern), Institution.name.ilike(pattern)))

        total = await self.session.scalar(
            select(func.count())
            .select_from(Department)
            .join(Institution, Institution.id == Department.institution_id)
            .where(*filters)
        )
        result = await self.session.execute(
            select(Department, Institution)
            .join(Institution, Institution.id == Department.institution_id)
            .where(*filters)
            .order_by(Institution.sort_order.asc(), Department.sort_order.asc())
        )
        return DepartmentListResponse(
            items=[
                DepartmentResponse(
                    id=department.id,
                    institution_id=institution.id,
                    institution_code=institution.code,
                    institution_name=institution.name,
                    name=department.name,
                    sort_order=department.sort_order,
                    is_active=department.is_active,
                    created_at=department.created_at,
                    updated_at=department.updated_at,
                )
                for department, institution in result.all()
            ],
            total=total or 0,
        )


async def ensure_institutions_seeded(session: AsyncSession) -> None:
    for sort_order, seed in enumerate(NULP_INSTITUTIONS, start=1):
        existing = await session.scalar(select(Institution).where(Institution.code == seed.code))
        if existing is None:
            session.add(
                Institution(
                    code=seed.code,
                    name=seed.name,
                    sort_order=sort_order,
                    is_active=True,
                )
            )
            continue
        existing.name = seed.name
        existing.sort_order = sort_order
        existing.is_active = True
    await session.flush()

    institution_ids_by_code = {
        institution.code: institution.id
        for institution in (
            await session.execute(select(Institution).where(Institution.is_active.is_(True)))
        )
        .scalars()
        .all()
    }
    department_counts_by_code: dict[str, int] = {}
    for seed in NULP_DEPARTMENTS:
        institution_id = institution_ids_by_code[seed.institution_code]
        department_counts_by_code[seed.institution_code] = (
            department_counts_by_code.get(seed.institution_code, 0) + 1
        )
        sort_order = department_counts_by_code[seed.institution_code]
        existing_department = await session.scalar(
            select(Department).where(
                Department.institution_id == institution_id,
                Department.name == seed.name,
            )
        )
        if existing_department is None:
            session.add(
                Department(
                    institution_id=institution_id,
                    name=seed.name,
                    sort_order=sort_order,
                    is_active=True,
                )
            )
            continue
        existing_department.sort_order = sort_order
        existing_department.is_active = True
    await session.commit()
