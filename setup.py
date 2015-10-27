from setuptools import setup, find_packages

setup(
    name='oocrawlers',
    version='0.11',
    description="OpenOil crawlers for the aleph system",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    keywords='',
    author='Friedrich Lindenberg',
    author_email='friedrich@pudo.org',
    url='http://openoil.net',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=False,
    install_requires=[],
    entry_points={
        'aleph.crawlers': [
            'edgar = oocrawlers.edgar:EdgarCrawler',
            'asx = oocrawlers.asx:ASXCrawler',
            'bombay = oocrawlers.bombay:BombayCrawler',
            'company_pdfs = oocrawlers.file_finder:BingPDFCrawler',
            'openoil = oocrawlers.openoil:OpenOilCrawler',
            'rigzone = oocrawlers.rigzone:RigZoneCrawler',
            'sedar = oocrawlers.sedar:SedarCrawler',
            'lse = oocrawlers.lse:LSECrawler',
            'edgar_oo = oocrawlers.sedar:OOEdgarCrawler',
            'internal_docs = oocrawlers.internaldocs:InternalDocsCrawler',
            'eiti = oocrawlers.eiti:EITICrawler',
            'singapore = oocrawlers.singapore:SingaporeCrawler',
            'south_africa = oocrawlers.south_africa:SouthAfricaCrawler',
        ]
    },
    tests_require=[]
)
