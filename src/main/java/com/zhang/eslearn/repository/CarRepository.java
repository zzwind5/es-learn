package com.zhang.eslearn.repository;

import org.springframework.data.repository.CrudRepository;

import com.zhang.eslearn.entity.CarEntity;

public interface CarRepository extends CrudRepository<CarEntity, String> {

}
