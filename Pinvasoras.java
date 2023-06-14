package pinvasoras;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import org.bson.Document;
import org.bson.types.ObjectId;


public class Pinvasoras {
    
    public static Connection Conexion() throws SQLException {
        String driver = "jdbc:postgresql:";
        String host = "//localhost:";
        String porto = "5432";
        String sid = "postgres";
        String usuario = "dam1a";
        String password = "castelao";
        String url = driver + host+ porto + "/" + sid;
        return DriverManager.getConnection(url,usuario,password);
    }
     
    public static MongoClient client;
    public static MongoDatabase database;
    public static MongoCollection<Document> colecion;
    
    public static void conectarServidor() {
        client = new MongoClient("localhost", 27017);
    }

    public static void conectarBase(String nomebase) {
        database = client.getDatabase(nomebase);
    }

    public static void conectarColecion(String coleccion) {
        colecion = database.getCollection(coleccion);      
    }

 
    public static void main(String[] args) throws SQLException {
        
        Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
        mongoLogger.setLevel(Level.SEVERE);
        conectarServidor();
        conectarBase("test");
        conectarColecion("especiesinvasoras");
        
        
        Connection conn = null;
        conn = Conexion();
        Statement stmt = conn.createStatement();
        
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("/home/dam1a/NetBeansProjects/Pinvasoras/encontradasezonas.odb");
        EntityManager em = emf.createEntityManager();
        
        
         int codzona = 0;
        //ejercicio 1
        TypedQuery<Encontradas> en = em.createQuery("select en from Encontradas en", Encontradas.class);
        List<Encontradas> r = en.getResultList();
        for(Encontradas encon : r){
            
            int numero = encon.getNumero();
            codzona = encon.getCodzona();
            int codespecie = encon.getCodespecie();
            double extension = encon.getExtension();
            
            System.out.println("numero: " + numero + "," + " codzona: " + codzona + "," + " codespecie: " + codespecie + "," + " extensión: " + extension);
            long tamano = r.size();


         
            //ejer3
            TypedQuery<Zonas> q2 = em.createQuery("select z from Zonas z where codz = "+ codzona+"", Zonas.class);
            Zonas r2 = q2.getSingleResult();
            int numeroinvasoras = r2.getNumeroinvasoras();
            numeroinvasoras = numeroinvasoras+1;
            r2.setNumeroinvasoras(numeroinvasoras);
            
            em.getTransaction().begin();
            em.persist(r2);
            
            em.getTransaction().commit();
            
     
       
          
            //ejer2
            TypedQuery<Zonas> z = em.createQuery("select zona from Zonas zona where codz="+ codzona +"", Zonas.class);
        List<Zonas> res = z.getResultList();
        for (Zonas zon : res){
            
            int codz = zon.getCodz();
            String nomz = zon.getNomz();
            int tempmedia = zon.getTempmedia();
            double superficie = zon.getSuperficie();
            numeroinvasoras = zon.getNumeroinvasoras();
            
            System.out.println("codz: " + codz + "," + " nomz:" + nomz + "," + " tempmedia: " + tempmedia + "," + " superficie: " + superficie + "," + " numeroinvasoras: " + numeroinvasoras);
        }
        
            //ejer4
            BasicDBObject condicion= new BasicDBObject("_id",codespecie);
            
            BasicDBObject campos = new BasicDBObject();
            campos.put("_id",1);
            campos.put("nomei",1);
            campos.put("tempbarrera", 1);
            
            Document d = colecion.find(condicion).projection(campos).first();
            
            
            Integer _id = d.getInteger("_id");
            String nomei = d.getString("nomei");
            Integer tempbarrera = d.getInteger("tempbarrera");
            
            System.out.println("_id: " +_id+  "," + " nomei: " + nomei + "," + " tempbarrera: " + tempbarrera);
            
            
            stmt = conn.createStatement();
            Document iterator;
            for(int t = 0; t<tamano;t++){
                MongoCursor<Document> temps = colecion.find(eq("_id", r.get(t).getCodespecie())).iterator();
            while(temps.hasNext()){
                iterator = temps.next();
                
            int tempBar = iterator.getInteger("tempbarrera");
                Double porcentaxeD = (r.get(t).getExtension()*100)/q2.get(z).getSuperficie();
                System.out.println(porcentaxeD);

                if(tempBar <q2.get(t).getTempmedia()){
                    String insertaPost = "INSERT INTO resumo (codz, nomez, nomei, danos) VALUES (" + r.get(t).getCodzona() + ", '"
                            + z.get(t).getNomz() + "', '" + nomei + "' , ( " + r.get(t).getExtension() + ", " + porcentaxeD
                            + "))";
                    stmt.executeUpdate(insertaPost);
                }
            }
        }
            
            
            
  
            
        }
        
        System.out.println("********************************************");
        
        //Final ejercicio 1
        
        //Principio ejercicio 2
         
       
        
        
        
        
        
        
        
        
        
        client.close();
        conn.close();
        em.close();
        emf.close();
    }
    
}}
