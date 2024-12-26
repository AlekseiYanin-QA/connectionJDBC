package dao;

import com.example.dao.TaskDao;
import com.example.entity.Task;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import javax.sql.DataSource;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaskDaoTest {
    private TaskDao taskDao;

    @BeforeAll
    public void setUp() throws IOException {
        DataSource dataSource = EmbeddedPostgres
                .builder()
                .start()
                .getPostgresDatabase();

        initializeDb(dataSource);
        taskDao = new TaskDao(dataSource);
    }

    @BeforeEach
    public void beforeEach() {
        taskDao.deleteAll();  // Сброс данных перед каждым тестом.
    }

    private void initializeDb(DataSource dataSource) {
        try (InputStream inputStream = this.getClass().getResource("/initial.sql").openStream()) {
            String sql = new String(inputStream.readAllBytes());
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.executeUpdate(sql);
            }
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSaveSetsId() {
        Task task = new Task("test task", false, LocalDateTime.now());
        taskDao.save(task);
        assertThat(task.getId()).isNotNull();  // Проверка, что ID присвоен.
    }

    @Test
    public void testFindAllReturnsAllTasks() {
        Task firstTask = new Task("first task", false, LocalDateTime.now());
        taskDao.save(firstTask);

        Task secondTask = new Task("second task", false, LocalDateTime.now());
        taskDao.save(secondTask);

        assertThat(taskDao.findAll())
                .hasSize(2)
                .extracting("id")
                .contains(firstTask.getId(), secondTask.getId());  // Проверка на наличие обоих задач.
    }

    @Test
    public void testDeleteAllDeletesAllRowsInTasks() {
        Task task = new Task("any task", false, LocalDateTime.now());
        taskDao.save(task);

        int rowsDeleted = taskDao.deleteAll();
        assertThat(rowsDeleted).isEqualTo(1);  // Проверка, что удалена одна запись.
        assertThat(taskDao.findAll()).isEmpty();  // Проверка, что в таблице не осталось записей.
    }

    @Test
    public void testGetByIdReturnsCorrectTask() {
        Task task = new Task("test task", false, LocalDateTime.now());
        taskDao.save(task); // Убедитесь, что этот метод присваивает ID задаче

        Task retrievedTask = taskDao.getById(task.getId());

        assertThat(retrievedTask)
                .isNotNull()
                .extracting("id", "title", "finished")
                .containsExactly(task.getId(), task.getTitle(), task.getFinished());

        // Сравнение без учета наносекунд
        LocalDateTime expectedDateTime = task.getCreatedDate().truncatedTo(ChronoUnit.MICROS);
        LocalDateTime actualDateTime = retrievedTask.getCreatedDate().truncatedTo(ChronoUnit.MICROS);

        assertThat(actualDateTime).isEqualTo(expectedDateTime);
    }

    @Test
    public void testFindNotFinishedReturnsCorrectTasks() {
        Task unfinishedTask = new Task("unfinished task", false, LocalDateTime.now());
        taskDao.save(unfinishedTask);

        Task finishedTask = new Task("finished task", true, LocalDateTime.now());
        taskDao.save(finishedTask);

        assertThat(taskDao.findAllNotFinished())
                .singleElement()
                .extracting("id", "title", "finished")
                .containsExactly(unfinishedTask.getId(), unfinishedTask.getTitle(), unfinishedTask.getFinished());

        // Округление до миллисекунд
        LocalDateTime expectedDateTime = unfinishedTask.getCreatedDate().truncatedTo(ChronoUnit.MILLIS);
        LocalDateTime actualDateTime = taskDao.findAllNotFinished().get(0).getCreatedDate().truncatedTo(ChronoUnit.MILLIS);

        assertThat(actualDateTime).isEqualTo(expectedDateTime);
    }

    @Test
    public void testFindNewestTasksReturnsCorrectTasks() {
        Task firstTask = new Task("first task", false, LocalDateTime.now().minusDays(2)); // Устанавливаем старую дату
        taskDao.save(firstTask);

        Task secondTask = new Task("second task", false, LocalDateTime.now().minusDays(1)); // Устанавливаем более новую дату
        taskDao.save(secondTask);

        Task thirdTask = new Task("third task", false, LocalDateTime.now()); // Устанавливаем самую новую дату
        taskDao.save(thirdTask);

        assertThat(taskDao.findNewestTasks(2))
                .hasSize(2)
                .extracting("id")
                .containsExactlyInAnyOrder(secondTask.getId(), thirdTask.getId());  // Проверка на наличие двух последних задач.
    }

    @Test
    public void testFinishSetsCorrectFlagInDb() {
        Task task = new Task("test task", false, LocalDateTime.now());
        taskDao.save(task);

        taskDao.finishTask(task);  // Обновляем статус задачи на завершенный.
        assertThat(taskDao.getById(task.getId()).getFinished()).isTrue();  // Проверка изменения статуса.
    }

    @Test
    public void deleteByIdDeletesOnlyNecessaryData() {
        Task taskToDelete = new Task("first task", false, LocalDateTime.now());
        taskDao.save(taskToDelete);

        Task taskToPreserve = new Task("second task", false, LocalDateTime.now());
        taskDao.save(taskToPreserve);

        taskDao.deleteById(taskToDelete.getId());  // Удаляем первую задачу.
        assertThat(taskDao.getById(taskToDelete.getId())).isNull();  // Проверка, что первая задача удалена.
        assertThat(taskDao.getById(taskToPreserve.getId())).isNotNull();  // Проверка, что вторая задача осталась.
    }
}
