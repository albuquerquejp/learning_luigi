import luigi
# Treino simples do pacote Luigi. 

class HelloWord (luigi.Task): #Primeira task com a finalidade de criar uma arquivo .txt com "Hello Word" escrito nele

    def output(self):
        return luigi.LocalTarget('hello.txt')

    def run(self):
        with self.output().open('w') as f:
            f.write("Hello World, esse é minha primeira task Luigi\n")

class ContarPalavras (luigi.Task): # Segunda task que 'pega' (requires) o output da primeira e computa quantos caracteres tem no arquivo.
    
    def requires(self):
        return HelloWord()
    
    def output(self):
        return luigi.LocalTarget('Contagem.txt')
    
    def run(self):
        
        with self.input().open('r') as ler:
            palavras = ler.read().split()
        
        with self.output().open('w') as f:
            for palavra in palavras:
                f.write(f'{palavra} | {len(palavra)}\n')

if __name__ == '__main__':
    luigi.run()


    
    
    
   
