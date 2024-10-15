import aiohttp
import asyncio
import csv
from bs4 import BeautifulSoup
import time

# Configuração de cabeçalho para as requisições HTTP
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
}

async def fetch(session, url):
    """Função assíncrona para obter conteúdo HTML da página."""
    async with session.get(url, headers=headers) as response:
        return await response.text()

async def extract_movie_details(session, movie_link):
    """Extrai e salva os detalhes de um filme específico."""
    try:
        html_content = await fetch(session, movie_link)
        movie_soup = BeautifulSoup(html_content, 'html.parser')

        title, date, rating, plot_text = None, None, None, None

        # Encontrando a seção principal
        page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})
        if page_section:
            divs = page_section.find_all('div', recursive=False)
            if len(divs) > 1:
                target_div = divs[1]

                # Extraindo o título
                title_tag = target_div.find('h1')
                if title_tag:
                    title = title_tag.find('span').get_text()

                # Extraindo a data de lançamento
                date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)
                if date_tag:
                    date = date_tag.get_text().strip()

                # Extraindo a classificação
                rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
                rating = rating_tag.get_text() if rating_tag else None

                # Extraindo a sinopse
                plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
                plot_text = plot_tag.get_text().strip() if plot_tag else None

                # Salvando no CSV
                with open('movies_async.csv', mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    if all([title, date, rating, plot_text]):
                        print(f'{title}, {date}, {rating}, {plot_text}')
                        writer.writerow([title, date, rating, plot_text])
    except Exception as e:
        print(f"Erro ao processar {movie_link}: {e}")

async def extract_movies():
    """Extrai links dos filmes mais populares do IMDB."""
    url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    async with aiohttp.ClientSession() as session:
        html_content = await fetch(session, url)
        soup = BeautifulSoup(html_content, 'html.parser')

        # Encontrando os links dos filmes
        movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
        movies_table_rows = movies_table.find_all('li')
        movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

        # Executando tarefas assíncronas para cada filme
        tasks = [extract_movie_details(session, link) for link in movie_links]
        await asyncio.gather(*tasks)

def main():
    """Função principal para medir o tempo e iniciar o scraping."""
    start_time = time.time()
    asyncio.run(extract_movies())
    end_time = time.time()
    print(f'Tempo total: {end_time - start_time:.2f} segundos')

if __name__ == '__main__':
    main()
