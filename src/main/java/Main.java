/**
 * Created by Igor on 3/28/2017.
 */

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import entities.Meal;
import entities.Pet;
import entities.User;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import mainwork.Cassandra;
import mainwork.Connection;
import ui.MainWindow;


public class Main {

    public static void main(String[] arg){
      /*  Cassandra c = new Cassandra();
     //   c.createUser("first", "user1", "user1");
        User u = new User();
        try {
            u = c.getUser("user1", "user1");
       //     c.createPet("Beauty", "Bunny", u);
            Set<Pet> p = c.getUsersPet(u);
            for (Pet pet: p){
                System.out.println(pet.toString());
            }
            //c.setUserBalance(u.userId, 700);
            c.getUserBalance(u.userId);
           // c.createMeal("Meat", "");
            List<Meal> m = c.getAllMeal();
          //  c.buyMeal(u.userId, m.get(0).mealId, 11, 3 );
            c.planMealPet(u.userId, UUID.fromString("d4305049-a88f-42be-8a79-d2dbcbc645f6"),m.get(0).mealId, 1);
            c.feedPet(u.userId, UUID.fromString("d4305049-a88f-42be-8a79-d2dbcbc645f6"),m.get(0).mealId, 1);
        }
        catch (Cassandra.NotFoundException ex){

        }
        System.out.println(u.toString());
       */
        MainWindow mainWindow = new  MainWindow();
        mainWindow.setVisible(true);
     //  Cassandra c =  Connection.getInstance();
       

    }
}