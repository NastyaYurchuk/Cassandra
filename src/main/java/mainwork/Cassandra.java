/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mainwork;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.reflect.TypeToken;
import entities.Fead;
import entities.Meal;
import entities.Pet;
import entities.Plan;
import entities.User;

import javax.xml.transform.Result;
import java.util.*;

 public class Cassandra {

    private static final String  serverIP = "127.0.0.1";
    private static final String keyspaceSystem = "system";
    private static final String keyspace = "labwork";

    private Cluster cluster;
    private Session session;

    Cassandra() {
        QueryOptions options= new QueryOptions();
        options.setConsistencyLevel(ConsistencyLevel.ALL);

        cluster = Cluster.builder()
                .withQueryOptions(options )
                .addContactPoints(serverIP)
                .build();

        session = cluster.connect(keyspaceSystem);

        // Create new keyspace
        String cqlStatement = "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH " +
                "replication = {'class':'SimpleStrategy','replication_factor':1}";

        session.execute(cqlStatement);

        //session.close();

        System.out.println("Connected successfully");
        createTables();
        System.out.println("Tables created");
       // session.close();
    }
    private void  createTables(){
        session = cluster.connect(keyspace);

        String cqlStatement = "create table if not exists ware_house_op(\n" +
"	op_id timeuuid,\n" +
"	user_id text,\n" +
"	meal_id text,\n" +
"	pet_id text,	\n" +
"	date timestamp,\n" +
"	amount int,\n" +
"	price float,\n" +
"	balance float,\n" +
"	remaind int,\n" +
"	fead_flg boolean,\n" +
"	PRIMARY KEY(user_id,op_id)\n" +
") with clustering order by (op_id desc);";

        session.execute(cqlStatement);

        cqlStatement = "create table if not exists users(\n" +
                "\tuser_id uuid,\n" +
                "\tuser_name text,\n" +
                "\tuser_login text,\n" +
                "\tuser_password text,\n" +
                "\tpets set<uuid>,\n" +
                "\tPRIMARY KEY (user_id)\n" +
                ");";
        session.execute(cqlStatement);

        cqlStatement = "create table if not exists pets(\n" +
                "\tpet_id uuid,\n" +
                "\tpet_name text,\n" +
                "\tpet_type text,\n" +
                "\tPRIMARY KEY (pet_id)\n" +
                ");";
        session.execute(cqlStatement);

        cqlStatement = "create table if not exists meals(\n" +
                "\tmeal_id uuid,\n" +
                "\tmeal_name text,\n" +
                "\tmeal_desc text,\n" +
                "\tPRIMARY KEY (meal_id)\n" +
                ");";
        session.execute(cqlStatement);
         cqlStatement = "CREATE INDEX IF NOT EXISTS meal ON ware_house_op (meal_id);" ;
        session.execute(cqlStatement);
        
        cqlStatement = "CREATE INDEX IF NOT EXISTS pet ON ware_house_op (pet_id);" ;
        session.execute(cqlStatement);
        
        cqlStatement = "create INDEX IF NOT EXISTS date on ware_house_op (date);" ;
        session.execute(cqlStatement);
        
        cqlStatement = "create index IF NOT EXISTS fead on ware_house_op (fead_flg);" ;
        session.execute(cqlStatement);
        
        cqlStatement = "create index IF NOT EXISTS login on users(user_login);" ;
        session.execute(cqlStatement);
        
        cqlStatement = "create index IF NOT EXISTS pass on users(user_password);" ;
        session.execute(cqlStatement);
        
        

        System.out.println("New table created");

    }

    public boolean createUser(String name, String login, String password ){
         try{
             String cqlStatement = "INSERT INTO "+keyspace + ".users(user_id,user_name,user_login,user_password)"+
                     "VALUES (uuid(),'"+ name + "','"+ login + "','" + password.hashCode() + "');";
             System.out.println(cqlStatement);
             session.execute(cqlStatement);
         }
         catch(Exception ex){
             return false;

         }
         return  true;
    }

    public User getUser(String user_login, String user_password) throws NotFoundException{
         User user = new User();
         
           Statement selectBuilder = QueryBuilder.select().from(keyspace,"users").allowFiltering().where(QueryBuilder.eq("user_login", user_login))
                   .and(QueryBuilder.eq("user_password", String.valueOf(user_password.hashCode())))
                   .setConsistencyLevel(ConsistencyLevel.ONE);
             System.out.println(selectBuilder.toString());
            ResultSet res = session.execute(selectBuilder);
            if(!res.isExhausted()){
                Row row = res.one();
                user.login = row.getString("user_login");
                user.userId = row.getUUID("user_id");
                user.name = row.getString("user_name");
                user.password= row.getString("user_password");
                user.pets = row.getSet("pets", UUID.class);

            } else {
                throw new NotFoundException("User not found");
            }

      
        return user;
    }

    public User updateUser(User user) throws NotFoundException{
        try{
            Statement selectBuilder = QueryBuilder.select().from(keyspace,"users")
                    .where(QueryBuilder.eq("user_id", user.userId))
                    .setConsistencyLevel(ConsistencyLevel.QUORUM);
            System.out.println(selectBuilder.toString());
            ResultSet res = session.execute(selectBuilder);
            if(!res.isExhausted()){
                Row row = res.one();
                user.login = row.getString("user_login");
                user.userId = row.getUUID("user_id");
                user.name = row.getString("user_name");
                user.password= row.getString("user_password");
                user.pets = row.getSet("pets", UUID.class);

            } else {
                throw new NotFoundException("User not found");
            }

        } catch(Exception ex){
            System.out.println(ex.toString());

        }
        return user;
    }

    public boolean createPet(String name, String type, User user){
        try{
           UUID pet_id = UUIDs.timeBased();
           String cqlStatement = "INSERT INTO "+keyspace + ".pets(pet_id,pet_name,pet_type)"+
                    "VALUES ("+pet_id+",'"+ name + "','"+ type +  "');";

           ResultSet res =  session.execute(cqlStatement);

            Set<UUID> pets = new LinkedHashSet<>(updateUser(user).pets);
            pets.add(pet_id);
            Statement updateStatemnt = QueryBuilder.update(keyspace, "users").with(QueryBuilder.set("pets", pets))
                    .where(QueryBuilder.eq("user_id", user.userId))
                    .setConsistencyLevel(ConsistencyLevel.QUORUM);
            System.out.println(updateStatemnt.toString());
           res =  session.execute(updateStatemnt);
        }
        catch(Exception ex){
            System.out.println(ex.toString());
            return false;

        }
        return  true;
    }

    public Pet getPet(String pet_id){
        Pet pet = new Pet();
        try{
            Statement selectBuilder = QueryBuilder.select().from(keyspace,"pets")
                    .where(QueryBuilder.eq("pet_id", UUID.fromString(pet_id)))
                    .setConsistencyLevel(ConsistencyLevel.ONE);
            System.out.println(selectBuilder.toString());
            ResultSet res = session.execute(selectBuilder);
            if(!res.isExhausted()){
                Row row = res.one();
                pet.petId = row.getUUID("pet_id");
                pet.name = row.getString("pet_name");
                pet.type = row.getString("pet_type");

            } else {
                throw new NotFoundException("Pet not found");
            }

        } catch(Exception ex){
            System.out.println(ex.toString());

        }
        return pet;
    };

    public List<Pet> getUsersPet(User user) throws NotFoundException{
        user = updateUser(user);
        List<Pet> set_pets = new ArrayList<Pet>();
        if(user.pets.size() > 0) {
            Statement selectStatement = QueryBuilder.select().all().from(keyspace, "pets")
                    .where(QueryBuilder.in("pet_id", user.pets.toArray()))
                    .setConsistencyLevel(ConsistencyLevel.ONE);
            System.out.println(selectStatement);
            ResultSet res = session.execute(selectStatement);

            while (!res.isExhausted()) {

                Row row = res.one();
                Pet temp_pet = new Pet();
                temp_pet.petId = row.getUUID("pet_id");
                temp_pet.name = row.getString("pet_name");
                temp_pet.type = row.getString("pet_type");
                set_pets.add(temp_pet);

            }
        }
        return set_pets;

    }

    public boolean createMeal(String name, String desc){
        try{
            final UUID meal_id = UUID.randomUUID();
            String cqlStatement = "INSERT INTO "+keyspace + ".meals(meal_id,meal_name, meal_desc)"+
                    "VALUES ("+meal_id+ ",'"+ name + "','"+ desc + "');";
            System.out.println(cqlStatement);
            session.execute(cqlStatement);
        }
        catch(Exception ex){
            return false;

        }
        return  true;

    }
    
    public List<Meal> getAllMeal(){
        List<Meal> listMeal = new ArrayList<>();

        Statement selectStatement = QueryBuilder.select().all().from(keyspace, "meals")
                .setConsistencyLevel(ConsistencyLevel.ONE);

        ResultSet res = session.execute(selectStatement);
        while(!res.isExhausted()){
           Meal temp_meal = new Meal();
           Row row = res.one();

           temp_meal.mealId = row.getUUID("meal_id");
           temp_meal.name = row.getString("meal_name");
           temp_meal.desc = row.getString("meal_desc");
           listMeal.add(temp_meal);
        }
        return listMeal;

    }

    public boolean setUserBalance(UUID user_id, float balance){
        try {
            Statement insertUpdate = QueryBuilder.insertInto("ware_house_op").value("op_id", UUIDs.timeBased())
                    .value("user_id", user_id.toString())
                    .value("meal_id", "")
                    .value("pet_id", "")
                    .value("date", new Date())
                    .value("balance", balance);
            System.out.println(insertUpdate);
            session.execute(insertUpdate);
        }catch (Exception ex){
            System.out.println(ex.toString());
            return false;
        }
        return true;
    }


    public float getUserBalance(UUID userId){
        float balance = 0;
        Statement selectStatement = QueryBuilder.select("balance").from(keyspace,"ware_house_op").allowFiltering()
                .where(QueryBuilder.eq("pet_id", ""))
                .and(QueryBuilder.eq("user_id", userId.toString())).limit(1);
        System.out.println(selectStatement);
        ResultSet res = session.execute(selectStatement);

        if(!res.isExhausted()){
            Row row = res.one();
            System.out.println(row.toString());
            balance = row.getFloat("balance");
        }
        return balance;
    }

    public boolean buyMeal(UUID userId, UUID mealId, float price, int amount) throws NotEnouqhResourceException{
 
            float balance = getUserBalance(userId);
            int remaind = getMealAmount(userId, mealId);
            if ((balance - price * amount) > 0) {
                float newBalance = balance - price * amount;
                Statement insertStatement = QueryBuilder.insertInto(keyspace, "ware_house_op").value("op_id", UUIDs.timeBased())
                        .value("user_id", userId.toString()).value("meal_id", mealId.toString())
                        .value("pet_id", "").value("amount", amount).value("price", price)
                        .value("balance", newBalance).value("remaind", remaind + amount).value("date", new Date())
                        .value("fead_flg", true);
                session.execute(insertStatement);
            } else{
                throw new NotEnouqhResourceException("Not enough money");
            }
            
        return true;
    }

    private int getMealAmount(UUID userId, UUID mealId) {
        int amount = 0;
        Statement selectStatement = QueryBuilder.select("remaind").from("ware_house_op").allowFiltering()
                .where(QueryBuilder.eq("user_id", userId.toString())).and(QueryBuilder.eq("meal_id",mealId.toString()))
                        .and(QueryBuilder.gt("date", -1)).limit(1);
        System.out.println(selectStatement);
        ResultSet res = session.execute(selectStatement);
        if (!res.isExhausted()){
            amount = res.one().getInt("remaind");
        }
        return amount;


    }

    public boolean planMealPet(UUID userId, UUID petId, UUID mealId, int amount){
        try{
            Statement insertStatement = QueryBuilder.insertInto(keyspace, "ware_house_op")
                    .value("op_id", UUIDs.timeBased()).value("user_id", userId.toString())
                    .value("meal_id", mealId.toString()).value("pet_id", petId.toString())
                    .value("amount",amount).value("date", -1).value("fead_flg", false);
            System.out.println(insertStatement);
            session.execute(insertStatement);


        }catch(Exception ex){
            System.out.println(ex.toString());
            return false;
        }
        return true;
    }

    public boolean feedPet(UUID userId,  UUID petId, UUID mealId, int amount) throws NotEnouqhResourceException{
        int remaind = getMealAmount(userId,mealId);
        System.out.println(remaind);
            if (remaind > 0) {
                int newAmount = ((remaind - amount) > 0) ? amount : remaind;
                int newRemaind = ((remaind - amount) > 0) ? (remaind - amount) : 0;
                Statement insertStatement = QueryBuilder.insertInto(keyspace, "ware_house_op")
                        .value("op_id", UUIDs.timeBased()).value("user_id", userId.toString())
                        .value("meal_id", mealId.toString()).value("pet_id", petId.toString())
                        .value("amount", newAmount).value("remaind", newRemaind)
                        .value("date", new Date()).value("fead_flg", true);
                session.execute(insertStatement);
            }
            else{
                throw new NotEnouqhResourceException("Not Enough meal");
            }
        
        return true;
    }

    public void updatePet(UUID petId, String name, String type) {
        try{
            Statement update = QueryBuilder.update(keyspace, "pets").with(QueryBuilder.set("pet_name", name))
                    .and(QueryBuilder.set("pet_type", type)).where(QueryBuilder.eq("pet_id", petId));
            session.execute(update);
        }catch (Exception ex){
            System.out.println(ex.toString());
        }
    }

    public void deletePet(Pet pet, User u) {
        try{
            List<UUID> op_ar = getPetOp(pet.petId);
            List<String> nameColumn = new ArrayList<>();
   
            BatchStatement batchStatement = new BatchStatement();
            Statement deletePets = QueryBuilder.delete().from(keyspace, "pets")
                    .where(QueryBuilder.eq("pet_id", pet.petId)).setConsistencyLevel(ConsistencyLevel.ALL);
            batchStatement.add(deletePets);
            for(UUID opId: op_ar){
                Statement deleteOp = QueryBuilder.delete().from(keyspace,"ware_house_op")
                        .where(QueryBuilder.eq("op_id", opId)).and(QueryBuilder.eq("user_id", u.userId.toString()))
                        .setConsistencyLevel(ConsistencyLevel.ALL);
                System.out.println(deleteOp);
                batchStatement.add(deleteOp);
            }
            
            session.execute(batchStatement);
            
        }catch (Exception ex){
            System.out.println(ex.toString());
        }
      
    }

    public List<Plan> getUsersPetPlan(UUID userId, UUID petId) {
         List<Plan> listPlan = new ArrayList<>();

        Statement selectStatement = QueryBuilder.select().all().from(keyspace, "ware_house_op").allowFiltering()
                .where(QueryBuilder.eq("user_id",userId.toString())).and(QueryBuilder.lt("op_id", UUIDs.timeBased()))
                .and(QueryBuilder.eq("pet_id", petId.toString()))
                .and(QueryBuilder.eq("date", -1));
        System.out.println(selectStatement.toString());
        ResultSet res = session.execute(selectStatement);
        while(!res.isExhausted()){
           Plan temp = new Plan();
           Row row = res.one();

           temp.mealId = row.getString("meal_id");
           temp.opId = row.getUUID("op_id");
           temp.petId = row.getString("pet_id");
           temp.userId = row.getString("user_id");
           temp.amount = row.getInt("amount");
           listPlan.add(temp);
        }
        return listPlan;
    }

    public String getMeal(String mealId) {
        String mealName = "";
        Statement selectStatement = QueryBuilder.select("meal_name").from(keyspace,"meals")
                .where(QueryBuilder.eq("meal_id", UUID.fromString(mealId)));
        
        ResultSet res = session.execute(selectStatement);
        if(!res.isExhausted()){
            mealName = res.one().getString("meal_name");
        }
        return mealName;
    }

    public List<Fead> getUsersPetFead(UUID userId, UUID petId) {
              List<Fead> listFead = new ArrayList<>();

        Statement selectStatement = QueryBuilder.select().all().from(keyspace, "ware_house_op").allowFiltering()
                .where(QueryBuilder.eq("user_id",userId.toString())).and(QueryBuilder.lt("op_id", UUIDs.timeBased()))
                .and(QueryBuilder.eq("pet_id", petId.toString()))
                .and(QueryBuilder.gt("date", -1));
        System.out.println(selectStatement.toString());
        ResultSet res = session.execute(selectStatement);
        while(!res.isExhausted()){
           Fead temp = new Fead();
           Row row = res.one();

           temp.mealId = row.getString("meal_id");
           temp.opId = row.getUUID("op_id");
           temp.petId = row.getString("pet_id");
           temp.userId = row.getString("user_id");
           temp.amount = row.getInt("amount");
           temp.date = row.getTimestamp("date");
           listFead.add(temp);
        }
        return listFead;
    }

    public Map<String, Integer> getWareHouse(User user) {
       Map<String, Integer> w = new TreeMap<>();
       Statement selectMeal = QueryBuilder.select("meal_id").from(keyspace, "ware_house_op").allowFiltering()
                .where(QueryBuilder.eq("user_id",user.userId.toString())).and(QueryBuilder.lt("op_id", UUIDs.timeBased()))
               .and(QueryBuilder.gt("meal_id", "")).and(QueryBuilder.eq("fead_flg", true));
        System.out.println(selectMeal.toString());
       ResultSet res = session.execute(selectMeal);
       
       for(Row row:res){
           
           if(!w.containsKey(row.getString("meal_id"))){
               w.put(row.getString("meal_id"), getMealAmount(user.userId, UUID.fromString(row.getString("meal_id"))));
           }
       }
       return w;        
    }
    
    public void closeSession(){
        this.session.close();
    }

    private List<UUID> getPetOp(UUID petId) {
         List<UUID> list_op = new ArrayList<>();
         
         Statement selectStatement = QueryBuilder.select("op_id").from(keyspace, "ware_house_op")
                 .where(QueryBuilder.eq("pet_id", petId.toString()));
         ResultSet res = session.execute(selectStatement);
         for(Row row: res){
             list_op.add(row.getUUID("op_id"));
         }
         return list_op;
    }

    public void buyCreateMeal(UUID userId, String name, float price, int amount) throws NotEnouqhResourceException {
         final UUID mealId = UUID.randomUUID();
            String cqlStatement = "INSERT INTO "+keyspace + ".meals(meal_id,meal_name, meal_desc)"+
                    "VALUES ("+mealId+ ",'"+ name + "','');";
            System.out.println(cqlStatement);
            session.execute(cqlStatement);
            buyMeal(userId, mealId, price, amount);
    }
    public Map<String, Set<String>>  getStatisticMeals(){
         String cqlStatement = "SELECT mealsPets(pet_id, meal_id) as m FROM ware_house_op";
         ResultSet resultSet = session.execute(cqlStatement);
         Set<String> s = new HashSet<>();
         TypeToken t = new TypeToken<Set<String>>(){};
         TypeToken tStr = new TypeToken<String>(){};
         Map<String, Set<String>> resWithId = resultSet.all().get(0).getMap(0, tStr, t);
         Map<String, Set<String>> resWithName = new HashMap<String, Set<String>>();
        
        for (Map.Entry<String, Set<String>> entry : resWithId.entrySet()) {
            System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
            String mealName = getMeal(entry.getKey());
            resWithName.put(mealName, getSetPet(entry.getValue()));


        }

         
       return  resWithName;
         
    }
    
    public String getMaxFead(){
        String name = "";
         String cqlStatement = "SELECT maxFeadPet(pet_id, amount, fead_flg) FROM ware_house_op;	";
         ResultSet resultSet = session.execute(cqlStatement);
       //  Map<String,Set<String>> m = resultSet.all().get(0).getMap(0, String.class, );
         String petId = resultSet.all().get(0).getString(0);
         name = getPet(petId).name + " the " + getPet(petId).type ;

         return name;
    }

    private Set<String> getSetPet(Set<String> SetId) {
        Set<String> petName = new HashSet<>();
        for(String id: SetId){
            Pet p = getPet(id);
            petName.add(p.type + " " + p.name);
        }
        return petName;
    }

    public class NotFoundException extends Exception {

        public NotFoundException(String message) {
            super(message);
        }


    }

    public class NotEnouqhResourceException extends Exception {

        public NotEnouqhResourceException(String message) {
            super(message);
        }


    }

}
