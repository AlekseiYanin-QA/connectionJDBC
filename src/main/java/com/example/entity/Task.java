package com.example.entity;

import java.time.LocalDateTime;

public class Task {
    private Integer id;
    private String title;
    private Boolean finished;
    private LocalDateTime createdDate;

    public Task(String title, Boolean finished, LocalDateTime createdDate) {
        this.title = title;
        this.finished = finished;
        this.createdDate = createdDate;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Boolean getFinished() {
        return finished;
    }

    public void setFinished(Boolean finished) {
        this.finished = finished;
    }

    public LocalDateTime getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(LocalDateTime createdDate) {
        this.createdDate = createdDate;
    }
}
